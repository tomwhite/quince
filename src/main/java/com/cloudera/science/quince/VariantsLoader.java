/*
 * Copyright (c) 2015, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.science.quince;

import java.io.IOException;
import java.util.Set;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Tuple3;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/*
 * Loads variants from input files (which may be VCF, Avro, or Parquet), converts
 * to an appropriate model (which may be ADAM or GA4GH) determined by the subclass,
 * then partitions by key (contig, pos, sample_group).
 */
public abstract class VariantsLoader {

  protected static PType<Tuple3<String, Long, String>> KEY_PTYPE =
      Avros.triples(Avros.strings(), Avros.longs(), Avros.strings());

  /**
   * Load variants and extract a key.
   * key = (contig, pos, sample_group); value = Variant/Call Avro object
   * @param inputFormat the format of the input data (VCF, AVRO, or PARQUET)
   * @param inputPath the input data path
   * @param conf the Hadoop configuration
   * @param pipeline the Crunch pipeline
   * @param variantsOnly whether to ignore samples and only load variants
   * @param flatten whether to flatten the data types
   * @param sampleGroup an identifier for the group of samples being loaded
   * @param samples the samples to include
   * @return the keyed variant or call records
   * @throws IOException if an I/O error is encountered during loading
   */
  protected abstract PTable<Tuple3<String, Long, String>, SpecificRecord>
      loadKeyedRecords(String inputFormat, Path inputPath, Configuration conf,
          Pipeline pipeline, boolean variantsOnly, boolean flatten, String sampleGroup,
          Set<String> samples)
      throws IOException;

  /**
   * Load and partition variants.
   * key = (contig, pos, sample_group); value = Variant/Call Avro object
   * @param inputFormat the format of the input data (VCF, AVRO, or PARQUET)
   * @param inputPath the input data path
   * @param conf the Hadoop configuration
   * @param pipeline the Crunch pipeline
   * @param variantsOnly whether to ignore samples and only load variants
   * @param flatten whether to flatten the data types
   * @param sampleGroup an identifier for the group of samples being loaded
   * @param samples the samples to include
   * @param redistribute whether to repartition the data by locus/sample group
   * @param segmentSize the number of base pairs in each segment partition
   * @param numReducers the number of reducers to use
   * @return the keyed variant or call records
   * @throws IOException if an I/O error is encountered during loading
   */
  public PTable<String, SpecificRecord> loadPartitionedVariants(
      String inputFormat, Path inputPath, Configuration conf,
      Pipeline pipeline, boolean variantsOnly, boolean flatten, String sampleGroup,
      Set<String> samples, boolean redistribute, long segmentSize, int numReducers)
      throws IOException {
    PTable<Tuple3<String, Long, String>, SpecificRecord> locusSampleKeyedRecords =
        loadKeyedRecords(inputFormat, inputPath, conf, pipeline, variantsOnly, flatten,
            sampleGroup, samples);

    // execute a DISTRIBUTE BY operation if requested
    PTable<Tuple3<String, Long, String>, SpecificRecord> sortedRecords;
    if (redistribute) {
      // partitionKey(chr, chrSeg, sampleGroup), Pair(secondaryKey/pos, originalDatum)
      PTableType<Tuple3<String, Long, String>,
          Pair<Long,
              Pair<Tuple3<String, Long, String>, SpecificRecord>>> reKeyedPType =
          Avros.tableOf(Avros.triples(Avros.strings(), Avros.longs(), Avros.strings()),
              Avros.pairs(Avros.longs(),
                  Avros.pairs(locusSampleKeyedRecords.getKeyType(),
                      locusSampleKeyedRecords.getValueType())));
      PTable<Tuple3<String, Long, String>,
          Pair<Long, Pair<Tuple3<String, Long, String>, SpecificRecord>>> reKeyed =
          locusSampleKeyedRecords.parallelDo("Re-keying for redistribution",
              new ReKeyDistributeByFn(segmentSize), reKeyedPType);
      // repartition and sort by pos
      sortedRecords = SecondarySort.sortAndApply(
          reKeyed, new UnKeyForDistributeByFn(),
          locusSampleKeyedRecords.getPTableType(), numReducers);
    } else {
      // input data assumed to be already globally sorted
      sortedRecords = locusSampleKeyedRecords;
    }

    // generate the partition keys
    return sortedRecords.mapKeys("Generate partition keys",
        new LocusSampleToPartitionFn(segmentSize, sampleGroup), Avros.strings());
  }

  /**
   * Function to change the key to (contig, segment, sample_group) and the value to
   * (pos, record).
   */
  public static final class ReKeyDistributeByFn
      extends MapFn<Pair<Tuple3<String, Long, String>, SpecificRecord>,
      Pair<Tuple3<String, Long, String>,
          Pair<Long, Pair<Tuple3<String, Long, String>, SpecificRecord>>>> {
    private long segmentSize;

    public ReKeyDistributeByFn(long segmentSize) {
      this.segmentSize = segmentSize;
    }

    @Override
    public Pair<Tuple3<String, Long, String>,
        Pair<Long, Pair<Tuple3<String, Long, String>, SpecificRecord>>> map(
        Pair<Tuple3<String, Long, String>, SpecificRecord> input) {
      Tuple3<String, Long, String> key = input.first();
      String chr = key.first();
      long pos = key.second();
      String sampleGroup = key.third();
      long segment = getRangeStart(segmentSize, pos);
      return Pair.of(Tuple3.of(chr, segment, sampleGroup), Pair.of(pos, input));
    }
  }

  /**
   * Function to reverse the effect of {@link ReKeyDistributeByFn}.
   */
  public static final class UnKeyForDistributeByFn
      extends DoFn<Pair<Tuple3<String, Long, String>,
      Iterable<Pair<Long, Pair<Tuple3<String, Long, String>,
          SpecificRecord>>>>,
      Pair<Tuple3<String, Long, String>, SpecificRecord>> {
    @Override
    public void process(
        Pair<Tuple3<String, Long, String>,
            Iterable<Pair<Long,
                Pair<Tuple3<String, Long, String>, SpecificRecord>>>> input,
        Emitter<Pair<Tuple3<String, Long, String>, SpecificRecord>> emitter) {
      for (Pair<Long,
          Pair<Tuple3<String, Long, String>, SpecificRecord>> pair : input.second()) {
        emitter.emit(pair.second());
      }
    }
  }

  /**
   * Function to map a record key to a Hive-compatible string representing the
   * partition key.
   */
  public static final class LocusSampleToPartitionFn
      extends MapFn<Tuple3<String, Long, String>, String> {
    private long segmentSize;
    private String sampleGroup;

    public LocusSampleToPartitionFn(long segmentSize, String sampleGroup) {
      this.segmentSize = segmentSize;
      this.sampleGroup = sampleGroup;
    }

    @Override
    public String map(Tuple3<String, Long, String> input) {
      StringBuilder sb = new StringBuilder();
      sb.append("chr=").append(input.first());
      sb.append("/pos=").append(getRangeStart(segmentSize, input.second()));
      if (sampleGroup != null) {
        sb.append("/sample_group=").append(sampleGroup);
      }
      return sb.toString();
    }
  }

  /**
   * @param size the size of the range
   * @param value the value that falls in a range
   * @return the start of the start (a multiple of <code>size</code>) that
   * <code>value</code> is in
   */
  public static long getRangeStart(long size, long value) {
    return Math.round(Math.floor(value / ((double) size))) * size;
  }
}
