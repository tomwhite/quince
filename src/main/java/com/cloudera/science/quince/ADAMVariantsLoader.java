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
import java.util.Collection;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.TableSource;
import org.apache.crunch.Tuple3;
import org.apache.crunch.io.From;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.bdgenomics.formats.avro.Genotype;
import org.bdgenomics.formats.avro.Variant;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;

public class ADAMVariantsLoader extends VariantsLoader {
  @Override
  public PTable<Tuple3<String, Long, String>, SpecificRecord>
    loadKeyedRecords(String inputFormat, Path inputPath, Configuration conf,
        Pipeline pipeline, boolean variantsOnly, boolean flatten, String sampleGroup)
        throws IOException {
    PCollection<Pair<org.bdgenomics.formats.avro.Variant, Collection<Genotype>>> adamRecords
        = readVariants(inputFormat, inputPath, conf, pipeline, sampleGroup);
    // The data are now loaded into ADAM variant objects; convert to keyed SpecificRecords
    ADAMToKeyedSpecificRecordFn converter =
        new ADAMToKeyedSpecificRecordFn(variantsOnly, flatten, sampleGroup);
    @SuppressWarnings("unchecked")
    PType<SpecificRecord> specificPType = Avros.specifics(converter.getSpecificRecordType());
    return adamRecords.parallelDo("Convert to keyed SpecificRecords",
        converter, Avros.tableOf(KEY_PTYPE, specificPType));
  }

  /*
   * Read input files (which may be VCF, Avro, or Parquet) and return a PCollection
   * of ADAM Variant/Genotype pairs.
   */
  private static PCollection<Pair<Variant, Collection<Genotype>>>
      readVariants(String inputFormat, Path inputPath, Configuration conf,
      Pipeline pipeline, String sampleGroup) throws IOException {
    PCollection<Pair<Variant, Collection<Genotype>>> adamRecords;
    if (inputFormat.equals("VCF")) {
      TableSource<LongWritable, VariantContextWritable> vcfSource =
          From.formattedFile(
              inputPath, VCFInputFormat.class, LongWritable.class, VariantContextWritable.class);
      PCollection<VariantContextWritable> vcfRecords = pipeline.read(vcfSource).values();
      PType<Pair<Variant, Collection<Genotype>>> adamPType =
          Avros.pairs(Avros.specifics(org.bdgenomics.formats.avro.Variant.class),
              Avros.collections(Avros.specifics(Genotype.class)));
      adamRecords =
          vcfRecords.parallelDo("VCF to ADAM Variant", new VCFToADAMVariantFn(), adamPType);
    } else if (inputFormat.equals("AVRO")) {
      throw new UnsupportedOperationException("Unsupported input format: " + inputFormat);
    } else if (inputFormat.equals("PARQUET")) {
      throw new UnsupportedOperationException("Unsupported input format: " + inputFormat);
    } else {
      throw new IllegalStateException("Unrecognized input format: " + inputFormat);
    }
    return adamRecords;
  }
}
