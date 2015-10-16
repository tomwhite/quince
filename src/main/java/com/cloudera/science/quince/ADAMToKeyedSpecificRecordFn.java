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

import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple4;
import org.bdgenomics.formats.avro.FlatGenotype;
import org.bdgenomics.formats.avro.FlatVariant;
import org.bdgenomics.formats.avro.Genotype;
import org.bdgenomics.formats.avro.Variant;

import java.util.Collection;

public class ADAMToKeyedSpecificRecordFn extends
    DoFn<Pair<org.bdgenomics.formats.avro.Variant, Collection<Genotype>>,
         Pair<Tuple4<String, Long, String, String>, SpecificRecord>> {
  private boolean variantsOnly;
  private boolean flatten;
  private String sampleGroup;

  public ADAMToKeyedSpecificRecordFn(boolean variantsOnly, boolean flatten, String sampleGroup) {
    this.variantsOnly = variantsOnly;
    this.flatten = flatten;
    this.sampleGroup = sampleGroup;
  }

  @Override
  public void process(Pair<org.bdgenomics.formats.avro.Variant, Collection<Genotype>> input,
                      Emitter<Pair<Tuple4<String, Long, String, String>, SpecificRecord>> emitter) {
    Variant variant = input.first();
    String contig = variant.getContig().getContigName();
    long pos = variant.getStart();
    if (variantsOnly) {
      Tuple4<String, Long, String, String> key = Tuple4.of(contig, pos, sampleGroup, null);
      SpecificRecord sr = flatten ? ADAMUtils.flattenVariant(variant) : variant;
      emitter.emit(Pair.of(key, sr));
    } else {  // genotype calls
      for (Genotype genotype : input.second()) {
        String sample = genotype.getSampleId();
        Tuple4<String, Long, String, String> key = Tuple4.of(contig, pos, sampleGroup, sample);
        SpecificRecord sr = flatten ? ADAMUtils.flattenGenotype(genotype) : genotype;
        emitter.emit(Pair.of(key, sr));
      }
    }
  }

  public Class getSpecificRecordType() {
    if (variantsOnly && flatten) {
      return FlatVariant.class;
    } else if (variantsOnly && !flatten) {
      return Variant.class;
    } else if (!variantsOnly && flatten) {
      return FlatGenotype.class;
    } else {  // !variantsOnly && !flatten
      return Genotype.class;
    }
  }
}
