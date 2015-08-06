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

import com.cloudera.science.moco.Converter;
import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.GenotypeLikelihoods;
import htsjdk.variant.variantcontext.GenotypesContext;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.ga4gh.models.Call;
import org.ga4gh.models.CallSet;
import org.ga4gh.models.Variant;
import org.ga4gh.models.VariantSet;
import org.ga4gh.models.VariantSetMetadata;

/**
 * Chr20 of 1000 genomes -> 855,196 rows  = 30 header, 855,166 rows
 * 
 * avro.snappy:  3GB ->  12.0 GB  (58 min)
 * avro.deflate: 3GB ->   7.2 GB  (82 min)
 * 
 * 
 * @author mh719
 *
 */
public class VariantContext2VariantConverter implements Converter<VariantContext, Variant> {

	private final AtomicReference<VariantConverterContext> ctx = new AtomicReference<VariantConverterContext>();

	/**
	 * Converts INFO, FORMAT, FILTER
	 * @param headerConverter
	 * @param header
	 * @param vs
	 */
	private static void convertHeaders(
			Converter<VCFHeaderLine, VariantSetMetadata> headerConverter,
			VCFHeader header, VariantSet vs) {
		Collection[] coll = new Collection[]{
				header.getInfoHeaderLines(),
				header.getFilterLines(),
				header.getFormatHeaderLines(),
//				header.getContigLines(),
				// TODO other formats
		};
		for(Collection<? extends VCFHeaderLine> c : coll){
			if(null != c){
				for(VCFHeaderLine hl : c){
					vs.getMetadata().add(
							headerConverter.forward(hl));
				}
			}
		}
	}

	public static void print (Variant v){
		System.out.println(String.format("%s:%s-%s %s %s %s",
				v.getReferenceName(),v.getStart(),v.getEnd(),v.getId(),
				v.getReferenceBases(),v.getAlternateBases()));
	}

	public VariantConverterContext getContext() {
		return ctx.get();
	}

	public void setContext(VariantConverterContext ctx){
		this.ctx.set(ctx);
	}

	/*
	 *
	 * VariantSetMetadata represents VCF header information.
	 *
	 * `Variant` and `CallSet` both belong to a `VariantSet`.
	 * `VariantSet` belongs to a `Dataset`.
	 * 	The variant set is equivalent to a VCF file.
	 *
	 * 	A `CallSet` is a collection of variant calls for a particular sample. It belongs to a `VariantSet`. This is equivalent to one column in VCF.
	 * 	A `Call` represents the determination of genotype with respect to a	particular `Variant`.
	 * 	An `AlleleCall` represents the determination of the copy number of a particular	`Allele`, possibly within a certain `Variant`.
	 * 	A `Variant` represents a change in DNA sequence relative to some reference. For example, a variant could represent a SNP or an insertion.
	 * 	Variants belong to a `VariantSet`. This is equivalent to a row in VCF.
	 *
	 * SUMMARY:
	 *  VCF File -> VariantSet
	 *  Header   -> VariantSetMetadata (separate converter)
	 *  Row -> Variant
	 *
	 *  Whole Column (sample) -> CallSet
	 *  One Column entry(sample) -> Call
	 *
	 *	*/

	@Override
	public Variant forward(VariantContext c) {
		Variant v = new Variant();

		v.setVariantSetId(""); // TODO
		// Assumed to be related to the avro record
		long currTime = System.currentTimeMillis();
		v.setCreated(currTime);
		v.setUpdated(currTime);

		/* VCF spec (1-Based):
		 *  -> POS - position: The reference position, with the 1st base having position 1.
		 *  -> TODO: Telomeres are indicated by using positions 0 or N+1
		 * GA4GH spec
		 *  -> (0-Based)
		 */
		v.setReferenceName(c.getChr());
		v.setStart(translateStartPosition(c));
		v.setEnd(/* 0-based, exclusive end position [start,end) */
			Long.valueOf(
				c.getEnd() /* 1-based, inclusive end position [start,end] */
				)); // TODO check if 0-based as well

		String rsId = StringUtils.EMPTY;
		List<CharSequence> nameList = Collections.emptyList();

		if(StringUtils.isNotBlank(c.getID())){
			rsId = c.getID();
			nameList = Collections.singletonList((CharSequence)rsId);
		}

		v.setId(rsId);
		v.setNames(nameList);

		/* Classic mode */
		// Reference
		Allele refAllele = c.getReference();
		v.setReferenceBases(refAllele.getBaseString());

		assert Long.compare(v.getEnd()-v.getStart(),v.getReferenceBases().length()) == 0;

		// Alt
		{
			List<Allele> altAllele = c.getAlternateAlleles();
			List<CharSequence> altList = new ArrayList<CharSequence>(altAllele.size());
			for(Allele a : altAllele ){
				altList.add(a.getBaseString());
			}
			v.setAlternateBases(altList);
		}


		/* Graph mode */
		// NOT supported
		// v.setAlleleIds(value);

		// QUAL
		/**
		 * TODO: QUAL - quality: Phred-scaled quality score
		 */

		//  FILTER
		/**
		 * TODO: FILTER - filter status
		 */

		// INFO
		v.setInfo(convertInfo(c));

		// Genotypes (Call's)

		v.setCalls(convertCalls(c));


		// TODO Auto-generated method stub
		return v;
	}

	/**
	 * Convert HTS Genotypes to GA4GH {@link org.ga4gh.models.Call}
	 * @param context {@link htsjdk.variant.variantcontext.VariantContext}
	 * @return {@link java.util.List} of {@link org.ga4gh.models.Call}
	 */
	@SuppressWarnings("deprecation")
	private List<Call> convertCalls(VariantContext context) {
		GenotypesContext gtc = context.getGenotypes();

		// Create Allele mapping
		Map<String, Integer> alleleMap = new HashMap<String, Integer>();
		int i = 0;
		alleleMap.put(context.getReference().getBaseString(), i++);
		for(Allele a : context.getAlternateAlleles()){
			alleleMap.put(a.getBaseString(), i++);
		}

		int size = gtc.size();
		List<Call> callList = new ArrayList<Call>(size);

		for(Genotype gt : gtc){
			String name = gt.getSampleName();
			CallSet cs = getContext().getCallSetMap().get(name);
			Call c = new Call();

			c.setCallSetId(cs.getId());
//			c.setCallSetName(cs.getName());  // TODO is this really needed 2x
			c.setCallSetName(StringUtils.EMPTY);

			List<Integer> cgt = new ArrayList<Integer>(gt.getPloidy());
			boolean unknownGT = false;
			// TODO issue with ./. -> propose ignore call
			for(Allele a : gt.getAlleles()){
				String baseString = a.getBaseString();
				Integer e = alleleMap.get(baseString);
				if(null == e){
					unknownGT = true;
					break;
				}
				cgt.add(e);
			}
			if(unknownGT){
				continue; // ignore Call -> Genotype not known (e.g. "./.")
			}
			c.setGenotype(cgt);
			c.setPhaseset(Boolean.valueOf(gt.isPhased()).toString()); // TODO decide what's the correct string

			Map<CharSequence, List<CharSequence>> infoMap = new HashMap<CharSequence, List<CharSequence>>();
			if(gt.hasAD()){
				int[] ad = gt.getAD();
				List<CharSequence> adl = new ArrayList<CharSequence>(ad.length);
				for(int val : ad){
					adl.add(Integer.toString(val));
				}
				infoMap.put(VCFConstants.GENOTYPE_ALLELE_DEPTHS, adl);
			}

			if(gt.hasDP()){
				infoMap.put(VCFConstants.DEPTH_KEY, Arrays.asList((CharSequence)Integer.toString(gt.getDP())));
			}

			if(gt.hasGQ()){
				infoMap.put(VCFConstants.GENOTYPE_QUALITY_KEY,Arrays.asList((CharSequence)Integer.toString(gt.getGQ())));
			}


			Map<String, Object> extAttr = gt.getExtendedAttributes();
			List<Double> lhList = Collections.emptyList();
			if(extAttr != null){
				for(Entry<String, Object> extEntry : extAttr.entrySet()){
					Object value = extEntry.getValue();
					// requires FullVCFCodec implementation to add GL as extra field instead of converting
					if(StringUtils.equals(extEntry.getKey(), VCFConstants.GENOTYPE_LIKELIHOODS_KEY)){
						if(null != value){
							GenotypeLikelihoods glField = GenotypeLikelihoods.fromGLField((String) value);
							double[] glv = glField.getAsVector();
							lhList = new ArrayList<Double>(glv.length);
							for(double d : glv){
								lhList.add(Double.valueOf(d));
							}
						}
					} else{
						if(value instanceof List){
							// TODO
						} else {
							infoMap.put(extEntry.getKey(), Collections.singletonList((CharSequence) value.toString()));
						}
					}
				}
			}
			c.setGenotypeLikelihood(lhList);

			c.setInfo(infoMap);
			callList.add(c);
		}
		return callList;
	}

	/**
	 * Conver into from HTS to GA4GH
	 * @param c {@link htsjdk.variant.variantcontext.VariantContext}
	 * @return Map of info information
	 */
	private Map<CharSequence, List<CharSequence>> convertInfo(VariantContext c) {
		Map<CharSequence, List<CharSequence>> infoMap = new HashMap<CharSequence, List<CharSequence>>();
		Map<String, Object> attributes = c.getAttributes();
		for(Entry<String, Object> a : attributes.entrySet()){
			String key = a.getKey();
			Object value = a.getValue();
			final List<CharSequence> valList;
			if(value == null){
				// don't do anything
				valList = Collections.emptyList();
			}else if(value instanceof List){
				List l = (List) value;
				valList = new ArrayList<CharSequence>(l.size());
				for(Object o : l){
					valList.add(o.toString());
				}
			} else {
				CharSequence o = value.toString();
				valList = Arrays.asList(o);
			}
			infoMap.put(key, valList);
		}
		return infoMap;
	}

	/**
	 * Translate from 1-based to 0-based
	 * @param c
	 * @return 0-based start position
	 */
	private Long translateStartPosition(VariantContext c) {
		int val = c.getStart();
		if(val == 0)
			return 0l; // TODO: test if this is correct for Telomeres
		return Long.valueOf(val-1);
	}

	@Override
	public VariantContext backward(Variant obj) {
		throw new NotImplementedException();
	}

}
