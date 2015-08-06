/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * 
 */
package com.cloudera.science.moco.variant;

import htsjdk.tribble.TribbleException;
import htsjdk.tribble.util.ParsingUtils;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeBuilder;
import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import htsjdk.variant.variantcontext.LazyGenotypesContext;
import htsjdk.variant.variantcontext.LazyGenotypesContext.LazyData;
import htsjdk.variant.vcf.VCFCodec;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeaderVersion;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author mh719
 *
 */
public class FullVcfCodec extends VCFCodec {
	

    private final String[] INT_DECODE_ARRAY = new String[10000];
    private final int[] decodeInts(final String string) {
        final int nValues = ParsingUtils.split(string, INT_DECODE_ARRAY, ',');
        final int[] values = new int[nValues];
        try {
            for ( int i = 0; i < nValues; i++ )
                values[i] = Integer.valueOf(INT_DECODE_ARRAY[i]);
        } catch (final NumberFormatException e) {
            return null;
        }
        return values;
    }
    
    public VCFHeaderVersion getVCFHeaderVersion(){
    	return this.version;
    }
	
	@Override
	public LazyData createGenotypeMap(String str, List<Allele> alleles,
			String chr, int pos) {
        if (genotypeParts == null)
            genotypeParts = new String[header.getColumnCount() - NUM_STANDARD_FIELDS];

        int nParts = ParsingUtils.split(str, genotypeParts, VCFConstants
            .FIELD_SEPARATOR_CHAR);
        if ( nParts != genotypeParts.length )
            generateException("there are " + (nParts-1) + " genotypes while the header requires that " + (genotypeParts.length-1) + " genotypes be present for all records at " + chr + ":" + pos, lineNo);

        ArrayList<Genotype> genotypes = new ArrayList<Genotype>(nParts);

        // get the format keys
        int nGTKeys = ParsingUtils.split(genotypeParts[0], genotypeKeyArray,
            VCFConstants.GENOTYPE_FIELD_SEPARATOR_CHAR);

        // cycle through the sample names
        Iterator<String> sampleNameIterator = header.getGenotypeSamples().iterator();

        // clear out our allele mapping
        alleleMap.clear();

        // cycle through the genotype strings
        for (int genotypeOffset = 1; genotypeOffset < nParts; genotypeOffset++) {
            int GTValueSplitSize = ParsingUtils.split(genotypeParts[genotypeOffset],
                GTValueArray, VCFConstants.GENOTYPE_FIELD_SEPARATOR_CHAR);

            final String sampleName = sampleNameIterator.next();
            final GenotypeBuilder gb = new GenotypeBuilder(sampleName);

            // check to see if the value list is longer than the key list, which is a problem
            if (nGTKeys < GTValueSplitSize)
                generateException("There are too many keys for the sample " + sampleName + ", keys = " + parts[8] + ", values = " + parts[genotypeOffset]);

            int genotypeAlleleLocation = -1;
            if (nGTKeys >= 1) {
                gb.maxAttributes(nGTKeys - 1);

                for (int i = 0; i < nGTKeys; i++) {
                    final String gtKey = genotypeKeyArray[i];
                    boolean missing = i >= GTValueSplitSize;

                    // todo -- all of these on the fly parsing of the missing value should be static constants
                    if (gtKey.equals(VCFConstants.GENOTYPE_KEY)) {
                        genotypeAlleleLocation = i;
                    } else if ( missing ) {
                        // if its truly missing (there no provided value) skip adding it to the attributes
                    } else if (gtKey.equals(VCFConstants.GENOTYPE_FILTER_KEY)) {
                        final List<String> filters = parseFilters(getCachedString(GTValueArray[i]));
                        if ( filters != null ) gb.filters(filters);
                    } else if ( GTValueArray[i].equals(VCFConstants.MISSING_VALUE_v4) ) {
                        // don't add missing values to the map
                    } else {
                        if (gtKey.equals(VCFConstants.GENOTYPE_QUALITY_KEY)) {
                            if ( GTValueArray[i].equals(VCFConstants.MISSING_GENOTYPE_QUALITY_v3) )
                                gb.noGQ();
                            else
                                gb.GQ((int)Math.round(Double.valueOf(GTValueArray[i])));
                        } else if (gtKey.equals(VCFConstants.GENOTYPE_ALLELE_DEPTHS)) {
                            gb.AD(decodeInts(GTValueArray[i]));
                        } else if (gtKey.equals(VCFConstants.GENOTYPE_PL_KEY)) {
                            gb.PL(decodeInts(GTValueArray[i]));
                        } else if (gtKey.equals(VCFConstants.GENOTYPE_LIKELIHOODS_KEY)) {
                            GenotypeLikelihoods glField = GenotypeLikelihoods.fromGLField(GTValueArray[i]);
							gb.PL(glField.getAsPLs());
							gb.attribute(gtKey,GTValueArray[i]);
                        } else if (gtKey.equals(VCFConstants.DEPTH_KEY)) {
                            gb.DP(Integer.valueOf(GTValueArray[i]));
                        } else {
                            gb.attribute(gtKey, GTValueArray[i]);
                        }
                    }
                }
            }

            // check to make sure we found a genotype field if our version is less than 4.1 file
            if ( ! version.isAtLeastAsRecentAs(VCFHeaderVersion.VCF4_1) && genotypeAlleleLocation == -1 )
                generateException("Unable to find the GT field for the record; the GT field is required before VCF4.1");
            if ( genotypeAlleleLocation > 0 )
                generateException("Saw GT field at position " + genotypeAlleleLocation + ", but it must be at the first position for genotypes when present");

            final List<Allele> GTalleles = (genotypeAlleleLocation == -1 ? new ArrayList<Allele>(0) : parseGenotypeAlleles(GTValueArray[genotypeAlleleLocation], alleles, alleleMap));
            gb.alleles(GTalleles);
            gb.phased(genotypeAlleleLocation != -1 && GTValueArray[genotypeAlleleLocation].indexOf(VCFConstants.PHASED) != -1);

            // add it to the list
            try {
                genotypes.add(gb.make());
            } catch (TribbleException e) {
                throw new TribbleException.InternalCodecException(e.getMessage() + ", at position " + chr+":"+pos);
            }
        }

        return new LazyGenotypesContext.LazyData(genotypes, header.getSampleNamesInOrder(), header.getSampleNameToOffset());
	}

}
