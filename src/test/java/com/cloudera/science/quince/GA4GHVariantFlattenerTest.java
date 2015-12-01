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

import com.google.common.collect.ImmutableList;
import org.ga4gh.models.Call;
import org.ga4gh.models.FlatVariantCall;
import org.ga4gh.models.Variant;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class GA4GHVariantFlattenerTest {

  @Test
  public void testVariantWithCalls() {
    Variant v = Variant.newBuilder()
        .setId(".")
        .setVariantSetId("")
        .setReferenceName("1")
        .setStart(14396L)
        .setEnd(14400L)
        .setReferenceBases("CTGT")
        .setAlternateBases(ImmutableList.<CharSequence>of("C"))
        .setAlleleIds(ImmutableList.<CharSequence>of("", ""))
        .setCalls(ImmutableList.of(
            Call.newBuilder()
                .setCallSetId("NA12878")
                .setGenotype(ImmutableList.of(0, 1))
                .setVariantId("")
                .build(),
            Call.newBuilder()
                .setCallSetId("NA12891")
                .setGenotype(ImmutableList.of(0, 1))
                .setVariantId("")
                .build()
        ))
        .build();

    FlatVariantCall flat1 = GA4GHVariantFlattener.flattenCall(v, v.getCalls().get(0));
    assertEquals(".", flat1.getId());
    assertEquals("1", flat1.getReferenceName());
    assertEquals(14396L, flat1.getStart().longValue());
    assertEquals(14400L, flat1.getEnd().longValue());
    assertEquals("CTGT", flat1.getReferenceBases());
    assertEquals("C", flat1.getAlternateBases1());
    assertEquals("NA12878", flat1.getCallSetId());
    assertEquals(0, flat1.getGenotype1().intValue());
    assertEquals(1, flat1.getGenotype2().intValue());

    FlatVariantCall flat2 = GA4GHVariantFlattener.flattenCall(v, v.getCalls().get(1));
    assertEquals(".", flat2.getId());
    assertEquals("1", flat2.getReferenceName());
    assertEquals(14396L, flat2.getStart().longValue());
    assertEquals(14400L, flat2.getEnd().longValue());
    assertEquals("CTGT", flat2.getReferenceBases());
    assertEquals("C", flat2.getAlternateBases1());
    assertEquals("NA12891", flat2.getCallSetId());
    assertEquals(0, flat2.getGenotype1().intValue());
    assertEquals(1, flat2.getGenotype2().intValue());
  }

  @Test
  public void testVariantOnly() {
    Variant v = Variant.newBuilder()
        .setId(".")
        .setVariantSetId("")
        .setReferenceName("1")
        .setStart(14396L)
        .setEnd(14400L)
        .setReferenceBases("CTGT")
        .setAlternateBases(ImmutableList.<CharSequence>of("C"))
        .setAlleleIds(ImmutableList.<CharSequence>of("", ""))
        .build();

    FlatVariantCall flat1 = GA4GHVariantFlattener.flattenVariant(v);
    assertEquals(".", flat1.getId());
    assertEquals("1", flat1.getReferenceName());
    assertEquals(14396L, flat1.getStart().longValue());
    assertEquals(14400L, flat1.getEnd().longValue());
    assertEquals("CTGT", flat1.getReferenceBases());
    assertEquals("C", flat1.getAlternateBases1());
    assertNull(flat1.getCallSetId());
    assertNull(flat1.getGenotype1());
    assertNull(flat1.getGenotype2());
  }
}
