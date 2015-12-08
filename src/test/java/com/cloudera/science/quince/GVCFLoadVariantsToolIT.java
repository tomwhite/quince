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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.ga4gh.models.FlatVariantCall;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemOutRule;
import parquet.avro.AvroParquetReader;
import parquet.hadoop.ParquetReader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class GVCFLoadVariantsToolIT {

  private LoadVariantsTool tool;

  @Rule
  public final SystemOutRule systemOutRule = new SystemOutRule().enableLog();

  @Before
  public void setUp() {
    tool = new LoadVariantsTool();
    Configuration conf = new Configuration();
    tool.setConf(conf);
  }

  @Test
  public void testExpandGvcfBlocks() throws Exception {

    String baseDir = "target/datasets";

    FileUtil.fullyDelete(new File(baseDir));

    String sampleGroup = "sample1";
    String input = "datasets/variants_gvcf";
    String output = "target/datasets/variants_flat_locuspart_gvcf";

    int exitCode = tool.run(new String[]{"--flatten", "--sample-group", sampleGroup,
        "--expand-gvcf-blocks", input, output});

    assertEquals(0, exitCode);
    File partition1 = new File(baseDir,
        "variants_flat_locuspart_gvcf/chr=20/pos=9000000/sample_group=sample1");
    assertTrue(partition1.exists());

    File[] dataFiles1 = partition1.listFiles(new HiddenFileFilter());

    assertEquals(1, dataFiles1.length);
    assertTrue(dataFiles1[0].getName().endsWith(".parquet"));

    ParquetReader<FlatVariantCall> parquetReader1 =
        AvroParquetReader.<FlatVariantCall>builder(new Path(dataFiles1[0].toURI())).build();

    // first record is a non-variant site record
    FlatVariantCall flat1 = parquetReader1.read();
    assertEquals(".", flat1.getId());
    assertEquals("20", flat1.getReferenceName());
    assertEquals(9999999L, flat1.getStart().longValue());
    assertEquals(10000000L, flat1.getEnd().longValue());
    assertEquals("T", flat1.getReferenceBases());
    assertEquals("", flat1.getAlternateBases1());
    assertEquals("NA12878", flat1.getCallSetId());
    assertEquals(0, flat1.getGenotype1().intValue());
    assertEquals(0, flat1.getGenotype2().intValue());

    checkSortedByStart(dataFiles1[0], 1);

    File partition2 = new File(baseDir,
        "variants_flat_locuspart_gvcf/chr=20/pos=10000000/sample_group=sample1");
    assertTrue(partition1.exists());

    File[] dataFiles2 = partition2.listFiles(new HiddenFileFilter());

    assertEquals(1, dataFiles2.length);
    assertTrue(dataFiles1[0].getName().endsWith(".parquet"));

    ParquetReader<FlatVariantCall> parquetReader2 =
        AvroParquetReader.<FlatVariantCall>builder(new Path(dataFiles2[0].toURI())).build();

    // second record is a non-variant site record, but in a new segment
    FlatVariantCall flat2 = parquetReader2.read();
    assertEquals(".", flat2.getId());
    assertEquals("20", flat2.getReferenceName());
    assertEquals(10000000L, flat2.getStart().longValue());
    assertEquals(10000001L, flat2.getEnd().longValue());
    assertEquals("", flat2.getReferenceBases());
    assertEquals("", flat2.getAlternateBases1());
    assertEquals("NA12878", flat2.getCallSetId());
    assertEquals(0, flat2.getGenotype1().intValue());
    assertEquals(0, flat2.getGenotype2().intValue());

    for (int i = 0; i < 114; i++) {
      parquetReader2.read();
    }

    // last record in the first block
    FlatVariantCall flat3 = parquetReader2.read();
    assertEquals(".", flat3.getId());
    assertEquals("20", flat3.getReferenceName());
    assertEquals(10000115L, flat3.getStart().longValue());
    assertEquals(10000116L, flat3.getEnd().longValue());
    assertEquals("", flat3.getReferenceBases());
    assertEquals("", flat3.getAlternateBases1());
    assertEquals("NA12878", flat3.getCallSetId());
    assertEquals(0, flat3.getGenotype1().intValue());
    assertEquals(0, flat3.getGenotype2().intValue());

    // regular variant call
    FlatVariantCall flat4 = parquetReader2.read();
    assertEquals(".", flat4.getId());
    assertEquals("20", flat4.getReferenceName());
    assertEquals(10000116L, flat4.getStart().longValue());
    assertEquals(10000117L, flat4.getEnd().longValue());
    assertEquals("C", flat4.getReferenceBases());
    assertEquals("T", flat4.getAlternateBases1());
    assertEquals("NA12878", flat4.getCallSetId());
    assertEquals(0, flat4.getGenotype1().intValue());
    assertEquals(1, flat4.getGenotype2().intValue());

    checkSortedByStart(dataFiles2[0], 1437);

  }

  private void checkSortedByStart(File file, int expectedCount) throws IOException {
    // check records are sorted by start position
    ParquetReader<FlatVariantCall> parquetReader =
        AvroParquetReader.<FlatVariantCall>builder(new Path(file.toURI())).build();

    int actualCount = 0;

    FlatVariantCall flat1 = parquetReader.read();
    actualCount++;

    Long previousStart = flat1.getStart();
    while (true) {
      FlatVariantCall flat = parquetReader.read();
      if (flat == null) {
        break;
      }
      Long start = flat.getStart();
      assertTrue("Should be sorted by start",
          previousStart.compareTo(start) <= 0);
      previousStart = start;
      actualCount++;
    }

    assertEquals(expectedCount, actualCount);
  }

  private static class HiddenFileFilter implements FileFilter {
    @Override
    public boolean accept(File pathname) {
      return !pathname.getName().startsWith(".");
    }
  }
}
