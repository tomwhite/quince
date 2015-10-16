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

import java.util.Arrays;

import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple4;
import org.ga4gh.models.Call;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;

/**
 * Convert a {@link VariantContextWritable} from a VCF file to a GA4GH
 * {@link org.ga4gh.models.Variant}.
 */
public class GA4GHToKeyedSpecificRecordFn
    extends DoFn<Variant, Pair<Tuple4<String, Long, String, String>, SpecificRecord>> {

  private boolean variantsOnly;
  private boolean flatten;
  private String sampleGroup;

  public GA4GHToKeyedSpecificRecordFn(boolean variantsOnly, boolean flatten, String sampleGroup) {
    this.variantsOnly = variantsOnly;
    this.flatten = flatten;
    this.sampleGroup = sampleGroup;
  }

  @Override
  public void process(Variant input,
                      Emitter<Pair<Tuple4<String, Long, String, String>, SpecificRecord>> emitter) {
    String contig = input.getReferenceName().toString();
    long pos = input.getStart();
    if (variantsOnly) {
      Tuple4<String, Long, String, String> key = Tuple4.of(contig, pos, sampleGroup, null);
      SpecificRecord sr = flatten ? GA4GHUtils.flattenVariant(input) : input;
      emitter.emit(Pair.of(key, sr));
    } else {  // genotype calls
      Variant.Builder variantBuilder = Variant.newBuilder(input).clearCalls();
      for (Call call : input.getCalls()) {
        Tuple4<String, Long, String, String> key =
            Tuple4.of(contig, pos, sampleGroup, call.getCallSetId().toString());
        variantBuilder.setCalls(Arrays.asList(call));
        Variant variant = variantBuilder.build();
        SpecificRecord sr = flatten ? GA4GHUtils.flattenCall(variant, call) : variant;
        emitter.emit(Pair.of(key, sr));
        variantBuilder.clearCalls();
      }
    }
  }

  public Class getSpecificRecordType() {
    if (variantsOnly && flatten) {
      return FlatVariantCall.class;
    } else if (variantsOnly && !flatten) {
      return Variant.class;
    } else if (!variantsOnly && flatten) {
      return FlatVariantCall.class;
    } else {  // !variantsOnly && !flatten
      return Variant.class;
    }
  }
}
