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
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
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
}
