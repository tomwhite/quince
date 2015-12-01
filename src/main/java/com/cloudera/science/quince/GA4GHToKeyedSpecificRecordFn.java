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

import java.util.Collections;
import org.apache.avro.specific.SpecificRecord;
import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.Pair;
import org.apache.crunch.Tuple3;
import org.ga4gh.models.Call;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;

/**
 * Extract the key from a GA4GH {@link Variant} and optionally flatten the record and
 * expand genotype calls.
 */
public class GA4GHToKeyedSpecificRecordFn
    extends DoFn<Variant, Pair<Tuple3<String, Long, String>, SpecificRecord>> {

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
                      Emitter<Pair<Tuple3<String, Long, String>, SpecificRecord>> emitter) {
    String contig = input.getReferenceName().toString();
    long pos = input.getStart();
    if (variantsOnly) {
      Tuple3<String, Long, String> key = Tuple3.of(contig, pos, sampleGroup);
      SpecificRecord sr = flatten ? GA4GHVariantFlattener.flattenVariant(input) : input;
      emitter.emit(Pair.of(key, sr));
    } else {  // genotype calls
      Variant.Builder variantBuilder = Variant.newBuilder(input).clearCalls();
      for (Call call : input.getCalls()) {
        Tuple3<String, Long, String> key = Tuple3.of(contig, pos, sampleGroup);
        variantBuilder.setCalls(Collections.singletonList(call));
        Variant variant = variantBuilder.build();
        SpecificRecord sr = flatten ? GA4GHVariantFlattener.flattenCall(variant, call) : variant;
        emitter.emit(Pair.of(key, sr));
        variantBuilder.clearCalls();
      }
    }
  }

  @Override
  public float scaleFactor() {
    // If the variant information is of size <i>v</i> (bytes) and the call information is
    // <i>c</i> then the scale factor is <i>n(v + c) / (v + nc)</i> for <i>n</i> calls.
    // If we assume that <i>v</i> is <i>2c</i> (as determined by a simple measurement),
    // then the scale factor works out at about 3.
    return variantsOnly ? super.scaleFactor() : 3.0f;
  }

  public Class getSpecificRecordType() {
    return flatten ? FlatVariantCall.class : Variant.class;
  }
}
