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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import java.util.Collection;
import java.util.List;

import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Pipeline;
import org.apache.crunch.PipelineResult;
import org.apache.crunch.Source;
import org.apache.crunch.TableSource;
import org.apache.crunch.Target;
import org.apache.crunch.Tuple3;
import org.apache.crunch.Tuple4;
import org.apache.crunch.impl.mr.MRPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.parquet.AvroParquetFileSource;
import org.apache.crunch.io.parquet.AvroParquetPathPerKeyTarget;
import org.apache.crunch.lib.SecondarySort;
import org.apache.crunch.types.PTableType;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.avro.Avros;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.bdgenomics.formats.avro.Genotype;
import org.ga4gh.models.Variant;
import org.seqdoop.hadoop_bam.VCFInputFormat;
import org.seqdoop.hadoop_bam.VariantContextWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads Variants stored in VCF, or Avro or Parquet GA4GH format, into a Hadoop
 * filesystem, ready for querying with Hive or Impala.
 */
@Parameters(commandDescription = "Load variants tool")
public class LoadVariantsTool extends Configured implements Tool {

  private static final Logger LOG = LoggerFactory
      .getLogger(LoadVariantsTool.class);

  @Parameter(description="<input-path> <output-path>")
  private List<String> paths;

  @Parameter(names="--data-model", description="The variant data model (GA4GH, or ADAM)")
  private String dataModel = "GA4GH";

  @Parameter(names="--input-format", description="Format of input data (VCF, AVRO, or PARQUET)")
  private String inputFormat = "VCF";

  @Parameter(names="--overwrite",
      description="Allow data for an existing sample group to be overwritten.")
  private boolean overwrite = false;

  @Parameter(names="--sample-group",
      description="An identifier for the group of samples being loaded.")
  private String sampleGroup = null;

  @Parameter(names="--variants-only",
      description="Ignore samples and only load variants.")
  private boolean variantsOnly = false;

  @Parameter(names="--segment-size",
      description="The number of base pairs in each segment partition.")
  private long segmentSize = 1000000;

  @Parameter(names="--redistribute",
      description="Whether to repartition the data by locus/sample group.")
  private boolean redistribute = false;

  @Parameter(names="--flatten",
      description="Whether to flatten the data types.")
  private boolean flatten = false;

  @Parameter(names="--num-reducers",
      description="The number of reducers to use.")
  private int numReducers = -1;

  @Override
  public int run(String[] args) throws Exception {
    JCommander jc = new JCommander(this);
    try {
      jc.parse(args);
    } catch (ParameterException e) {
      jc.usage();
      return 1;
    }

    if (paths == null || paths.size() != 2) {
      jc.usage();
      return 1;
    }

    String inputPathString = paths.get(0);
    String outputPathString = paths.get(1);

    Configuration conf = getConf();
    Path inputPath = new Path(inputPathString);
    Path outputPath = new Path(outputPathString);
    outputPath = outputPath.getFileSystem(conf).makeQualified(outputPath);

    Pipeline pipeline = new MRPipeline(getClass(), conf);

    PType<Tuple4<String, Long, String, String>> keyPType =
        Avros.quads(Avros.strings(), Avros.longs(), Avros.strings(), Avros.strings());
    // key = (contig, pos, sample_group, sample); value = Variant/Call Avro object
    PTable<Tuple4<String, Long, String, String>, SpecificRecord> locusSampleKeyedRecords;
    if (dataModel.equals("GA4GH")) {
      PCollection<Variant> ga4ghRecords;
      if (inputFormat.equals("VCF")) {
        VCFToGA4GHVariantFn.configureHeaders(
            conf, FileUtils.findVcfs(inputPath, conf), sampleGroup);
        TableSource<LongWritable, VariantContextWritable> vcfSource =
            From.formattedFile(
                inputPath, VCFInputFormat.class, LongWritable.class, VariantContextWritable.class);
        PCollection<VariantContextWritable> vcfRecords = pipeline.read(vcfSource).values();
        ga4ghRecords = vcfRecords.parallelDo(
            "VCF to GA4GH Variant", new VCFToGA4GHVariantFn(), Avros.specifics(Variant.class));
      } else if (inputFormat.equals("AVRO")) {
        ga4ghRecords = pipeline.read(From.avroFile(inputPath, Avros.specifics(Variant.class)));
      } else if (inputFormat.equals("PARQUET")) {
        @SuppressWarnings("unchecked")
        Source<Variant> source =
            new AvroParquetFileSource(inputPath, Avros.specifics(Variant.class));
        ga4ghRecords = pipeline.read(source);
      } else {
        jc.usage();
        return 1;
      }
      // The data are now loaded into GA4GH variant objects; convert to keyed SpecificRecords
      GA4GHToKeyedSpecificRecordFn converter =
          new GA4GHToKeyedSpecificRecordFn(variantsOnly, flatten, sampleGroup);
      @SuppressWarnings("unchecked")
      PType<SpecificRecord> specificPType = Avros.specifics(converter.getSpecificRecordType());
      locusSampleKeyedRecords = ga4ghRecords.parallelDo("Convert to keyed SpecificRecords",
          converter, Avros.tableOf(keyPType, specificPType));
    } else if (dataModel.equals("ADAM")) {
      PCollection<Pair<org.bdgenomics.formats.avro.Variant, Collection<Genotype>>> adamRecords;
      if (inputFormat.equals("VCF")) {
        TableSource<LongWritable, VariantContextWritable> vcfSource =
            From.formattedFile(
                inputPath, VCFInputFormat.class, LongWritable.class, VariantContextWritable.class);
        PCollection<VariantContextWritable> vcfRecords = pipeline.read(vcfSource).values();
        PType<Pair<org.bdgenomics.formats.avro.Variant, Collection<Genotype>>> adamPType =
            Avros.pairs(Avros.specifics(org.bdgenomics.formats.avro.Variant.class),
                        Avros.collections(Avros.specifics(Genotype.class)));
        adamRecords =
            vcfRecords.parallelDo("VCF to ADAM Variant", new VCFToADAMVariantFn(), adamPType);
      } else if (inputFormat.equals("AVRO")) {
        // TODO
        jc.usage();
        return 1;
      } else if (inputFormat.equals("PARQUET")) {
        // TODO
        jc.usage();
        return 1;
      } else {
        jc.usage();
        return 1;
      }
      // The data are now loaded into ADAM variant objects; convert to keyed SpecificRecords
      ADAMToKeyedSpecificRecordFn converter =
          new ADAMToKeyedSpecificRecordFn(variantsOnly, flatten, sampleGroup);
      @SuppressWarnings("unchecked")
      PType<SpecificRecord> specificPType = Avros.specifics(converter.getSpecificRecordType());
      locusSampleKeyedRecords = adamRecords.parallelDo("Convert to keyed SpecificRecords",
          converter, Avros.tableOf(keyPType, specificPType));
    } else {
      jc.usage();
      return 1;
    }

    // execute a DISTRIBUTE BY operation if requested
    PTable<Tuple4<String, Long, String, String>, SpecificRecord> sortedRecords;
    if (redistribute) {
      // partitionKey(chr, chrSeg, sampleGroup), Pair(secondaryKey/pos, originalDatum)
      PTableType<Tuple3<String, Long, String>,
                 Pair<Long,
                      Pair<Tuple4<String, Long, String, String>,
                           SpecificRecord>>> reKeyedPType =
          Avros.tableOf(Avros.triples(Avros.strings(), Avros.longs(), Avros.strings()),
                        Avros.pairs(Avros.longs(),
                                    Avros.pairs(locusSampleKeyedRecords.getKeyType(),
                                                locusSampleKeyedRecords.getValueType())));
      PTable<Tuple3<String, Long, String>,
             Pair<Long, Pair<Tuple4<String, Long, String, String>, SpecificRecord>>> reKeyed =
          locusSampleKeyedRecords.parallelDo("Re-keying for redistribution",
              new CrunchUtils.ReKeyDistributeByFn(segmentSize), reKeyedPType);
      // repartition and sort by pos
      sortedRecords = SecondarySort.sortAndApply(
          reKeyed, new CrunchUtils.UnKeyForDistributeByFn(),
          locusSampleKeyedRecords.getPTableType(), numReducers);
    } else {
      // input data assumed to be already globally sorted
      sortedRecords = locusSampleKeyedRecords;
    }

    // generate the partition keys
    PTable<String, SpecificRecord> partitionKeyedRecords = sortedRecords
        .mapKeys("Generate partition keys",
            new CrunchUtils.LocusSampleToPartitionFn(segmentSize, sampleGroup), Avros.strings());

    if (FileUtils.sampleGroupExists(outputPath, conf, sampleGroup)) {
      if (overwrite) {
        FileUtils.deleteSampleGroup(outputPath, conf, sampleGroup);
      } else {
        LOG.error("Sample group already exists: " + sampleGroup);
        return 1;
      }
    }

    pipeline.write(partitionKeyedRecords, new AvroParquetPathPerKeyTarget(outputPath),
        Target.WriteMode.APPEND);

    PipelineResult result = pipeline.done();
    return result.succeeded() ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new LoadVariantsTool(), args);
    System.exit(exitCode);
  }

}
