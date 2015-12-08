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
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.Tuple3;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

public class GA4GHVariantsLoader extends VariantsLoader {

  @Override
  public PTable<Tuple3<String, Long, String>, SpecificRecord>
      loadKeyedRecords(String inputFormat, Path inputPath, Configuration conf,
          Pipeline pipeline, boolean variantsOnly, boolean flatten, String sampleGroup,
          Set<String> samples)
      throws IOException {
    PCollection<Variant> variants = readVariants(inputFormat, inputPath,
        conf, pipeline, sampleGroup);

    GA4GHToKeyedSpecificRecordFn converter =
        new GA4GHToKeyedSpecificRecordFn(variantsOnly, flatten, sampleGroup, samples);
    @SuppressWarnings("unchecked")
    PType<SpecificRecord> specificPType = Avros.specifics(converter
        .getSpecificRecordType());
    return variants.parallelDo("Convert to keyed SpecificRecords",
        converter, Avros.tableOf(KEY_PTYPE, specificPType));
  }

  /*
   * Read input files (which may be VCF, Avro, or Parquet) and return a PCollection
   * of GA4GH Variant objects.
   */
  private static PCollection<Variant> readVariants(String inputFormat, Path inputPath,
      Configuration conf, Pipeline pipeline, String sampleGroup) throws IOException {
    PCollection<Variant> variants;
    if (inputFormat.equals("VCF")) {
      VCFToGA4GHVariantFn.configureHeaders(
          conf, FileUtils.findVcfs(inputPath, conf), sampleGroup);
      TableSource<LongWritable, VariantContextWritable> vcfSource =
          From.formattedFile(
              inputPath, VCFInputFormat.class, LongWritable.class, VariantContextWritable.class);
      PCollection<VariantContextWritable> vcfRecords = pipeline.read(vcfSource).values();
      variants = vcfRecords.parallelDo(
          "VCF to GA4GH Variant", new VCFToGA4GHVariantFn(), Avros.specifics(Variant.class));
    } else if (inputFormat.equals("AVRO")) {
      variants = pipeline.read(From.avroFile(inputPath, Avros.specifics(Variant.class)));
    } else if (inputFormat.equals("PARQUET")) {
      @SuppressWarnings("unchecked")
      Source<Variant> source =
          new AvroParquetFileSource(inputPath, Avros.specifics(Variant.class));
      variants = pipeline.read(source);
    } else {
      throw new IllegalStateException("Unrecognized input format: " + inputFormat);
    }
    return variants;
  }

  @Override
  protected PTable<Tuple3<String, Long, String>, SpecificRecord>
      expandGvcfBlocks(PTable<Tuple3<String, Long, String>, SpecificRecord> records,
      long segmentSize) {
    return records.parallelDo(new ExpandGvcfBlocksFn(segmentSize), records.getPTableType());
  }

  public static final class ExpandGvcfBlocksFn
      extends DoFn<Pair<Tuple3<String, Long, String>, SpecificRecord>,
      Pair<Tuple3<String, Long, String>, SpecificRecord>> {

    private long segmentSize;

    public ExpandGvcfBlocksFn(long segmentSize) {
      this.segmentSize = segmentSize;
    }

    @Override
    public void process(Pair<Tuple3<String, Long, String>, SpecificRecord> input,
        Emitter<Pair<Tuple3<String, Long, String>, SpecificRecord>> emitter) {

      SpecificRecord record = input.second();
      if (record instanceof FlatVariantCall) {
        FlatVariantCall v = (FlatVariantCall) record;
        boolean block = v.getAlternateBases1().equals("") // <NON_REF>
            && v.getAlternateBases2() == null;
        if (block) {
          long start = v.getStart();
          long end = v.getEnd();
          long segmentStart = getRangeStart(segmentSize, start);
          long segmentEnd = getRangeStart(segmentSize, end);
          if (segmentStart == segmentEnd) { // block is contained in one segment
            for (long pos = start; pos < end; pos++) {
              // mutate start and end - this is OK since Parquet will write out the
              // values immediately
              v.setStart(pos);
              v.setEnd(pos + 1);
              if (pos > start) {
                // set reference to unknown (TODO: broadcast ref so we can set correctly)
                v.setReferenceBases(""); // set reference to unknown
              }
              emitter.emit(input);
            }
          } else { // block spans multiple segments, so need to update key with correct pos
            Tuple3<String, Long, String> key = input.first();
            for (long pos = start; pos < end; pos++) {
              // mutate start and end - this is OK since Parquet will write out the
              // values immediately
              v.setStart(pos);
              v.setEnd(pos + 1);
              if (pos > start) {
                // set reference to unknown (TODO: broadcast ref so we can set correctly)
                v.setReferenceBases(""); // set reference to unknown
              }
              Tuple3<String, Long, String> newKey =
                  Tuple3.of(key.first(), getRangeStart(segmentSize, pos), key.third());
              emitter.emit(Pair.of(newKey, (SpecificRecord) v));
            }
          }
          return;
        }
      }
      // TODO: Variant
      emitter.emit(input);
    }
  }
}
