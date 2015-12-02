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

import java.util.List;
import org.bdgenomics.formats.avro.FlatGenotype;
import org.bdgenomics.formats.avro.FlatVariant;
import org.bdgenomics.formats.avro.Genotype;
import org.bdgenomics.formats.avro.GenotypeAllele;
import org.bdgenomics.formats.avro.Variant;

public final class ADAMVariantFlattener {
  private ADAMVariantFlattener() {
  }

  public static FlatVariant flattenVariant(Variant variant) {
    FlatVariant.Builder fvb = FlatVariant.newBuilder();
    fvb.setVariantErrorProbability(variant.getVariantErrorProbability());
    fvb.setContigName(variant.getContig().getContigName());
    fvb.setStart(variant.getStart());
    fvb.setEnd(variant.getEnd());
    fvb.setReferenceAllele(variant.getReferenceAllele());
    fvb.setAlternateAllele(variant.getAlternateAllele());
    fvb.setIsSomatic(variant.getIsSomatic());
    return fvb.build();
  }

  public static FlatGenotype flattenGenotype(Genotype genotype) {
    FlatGenotype.Builder fgb = FlatGenotype.newBuilder();
    // first set the denormalized Variant fields
    fgb.setVariantErrorProbability(genotype.getVariant().getVariantErrorProbability());
    fgb.setContigName(genotype.getVariant().getContig().getContigName());
    fgb.setStart(genotype.getVariant().getStart());
    fgb.setEnd(genotype.getVariant().getEnd());
    fgb.setReferenceAllele(genotype.getVariant().getReferenceAllele());
    fgb.setAlternateAllele(genotype.getVariant().getAlternateAllele());
    fgb.setIsSomatic(genotype.getVariant().getIsSomatic());
    // then set the flattened genotype fields
    fgb.setSampleId(genotype.getSampleId());
    fgb.setSampleDescription(genotype.getSampleDescription());
    fgb.setProcessingDescription(genotype.getProcessingDescription());
    fgb.setAllele1(convertAlleleToDiploidCode(get(genotype.getAlleles(), 0)));
    fgb.setAllele2(convertAlleleToDiploidCode(get(genotype.getAlleles(), 1)));
    fgb.setExpectedAlleleDosage(genotype.getExpectedAlleleDosage());
    fgb.setReferenceReadDepth(genotype.getReferenceReadDepth());
    fgb.setAlternateReadDepth(genotype.getAlternateReadDepth());
    fgb.setReadDepth(genotype.getReadDepth());
    fgb.setMinReadDepth(genotype.getMinReadDepth());
    fgb.setGenotypeQuality(genotype.getGenotypeQuality());
    fgb.setGenotypeLikelihood0(get(genotype.getGenotypeLikelihoods(), 0));
    fgb.setGenotypeLikelihood1(get(genotype.getGenotypeLikelihoods(), 1));
    fgb.setGenotypeLikelihood2(get(genotype.getGenotypeLikelihoods(), 2));
    fgb.setNonReferenceLikelihood0(get(genotype.getNonReferenceLikelihoods(), 0));
    fgb.setNonReferenceLikelihood1(get(genotype.getNonReferenceLikelihoods(), 1));
    fgb.setNonReferenceLikelihood2(get(genotype.getNonReferenceLikelihoods(), 2));
    fgb.setStrandBiasComponent0(get(genotype.getStrandBiasComponents(), 0));
    fgb.setStrandBiasComponent1(get(genotype.getStrandBiasComponents(), 1));
    fgb.setStrandBiasComponent2(get(genotype.getStrandBiasComponents(), 2));
    fgb.setStrandBiasComponent3(get(genotype.getStrandBiasComponents(), 3));
    fgb.setSplitFromMultiAllelic(genotype.getSplitFromMultiAllelic());
    fgb.setIsPhased(genotype.getIsPhased());
    fgb.setPhaseSetId(genotype.getPhaseSetId());
    fgb.setPhaseQuality(genotype.getPhaseQuality());
    return fgb.build();
  }

  private static Integer convertAlleleToDiploidCode(GenotypeAllele ga) {
    // see flat-adam.avdl for encoding
    if (ga == null) {
      return null;
    } else if (ga == GenotypeAllele.Ref) {
      return 0;
    } else if (ga == GenotypeAllele.Alt) {
      return 1;
    } else if (ga == GenotypeAllele.OtherAlt) {
      return 2;
    } else if (ga == GenotypeAllele.NoCall) {
      return 4;
    } else {
      throw new IllegalStateException("Invalid ordinal for GenotypeAllele enum");
    }
  }

  private static <T> T get(List<T> names, int index) {
    if (names == null) {
      return null;
    }
    return index < names.size() ? names.get(index) : null;
  }
}
