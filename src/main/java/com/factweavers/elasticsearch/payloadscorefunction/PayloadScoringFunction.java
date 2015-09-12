/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.factweavers.elasticsearch.payloadscorefunction;

import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.search.lookup.IndexField;
import org.elasticsearch.search.lookup.IndexFieldTerm;
import org.elasticsearch.search.lookup.IndexLookup;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * A function_score function that multiplies the score with the value of a field
 * from the document, optionally multiplying the field by a factor first, and
 * applying a modification (log, ln, sqrt, square, etc) afterwards.
 */
public class PayloadScoringFunction extends ScoreFunction {
	private final String field;
	private final List<String> values;
	private float defaultValue;
	private IndexLookup indexLookup;
	//private ESLogger logger = Loggers.getLogger(PayloadScoringFunction.class);

	public PayloadScoringFunction(IndexLookup indexLookup, String field,
			List<String> values, float defaultValue) {
		super(CombineFunction.MULT);
		this.field = field;
		this.values = values;
		this.defaultValue = defaultValue;
		this.indexLookup = indexLookup;
	}

	@Override
	public void setNextReader(AtomicReaderContext context) {
		indexLookup.setNextReader(context);
	}

	@Override
	public double score(int docId, float subQueryScore) {
		indexLookup.setNextDocId(docId);
		float score = 0;
		try {
			Fields termVectors = indexLookup.termVectors();
			Boolean isPayloadOrIndex = false;
			TermsEnum iterator = null;
			if (termVectors != null && termVectors.terms(field) != null
					&& termVectors.terms(field).hasPayloads()) {
				isPayloadOrIndex = true;
				Terms fields = termVectors.terms(field);
				iterator = fields.iterator(null);
			}

			if (isPayloadOrIndex) {
				BytesRef firstElement = iterator.next();
				while (firstElement != null) {
					String currentValue = firstElement.utf8ToString();
					if (!values.contains(currentValue)) {
						//logger.info("Payload Skipping " + currentValue);
						firstElement = iterator.next();
						continue;
					}
					//logger.info("Payload processing value is " + currentValue);
					DocsAndPositionsEnum docsAndPositions = iterator
							.docsAndPositions(null, null);
					docsAndPositions.nextDoc();
					docsAndPositions.nextPosition();
					BytesRef payload = docsAndPositions.getPayload();
					if (payload != null) {
						score += PayloadHelper.decodeFloat(payload.bytes,
								payload.offset);
						//logger.info("Score " + score);
					}
					else{
						score += defaultValue;
					}
					firstElement = iterator.next();
				}
			} else {
				IndexField fieldObject = indexLookup.get(field);
				for (String value : values) {
					IndexFieldTerm tokens = fieldObject.get(value,
							IndexLookup.FLAG_CACHE | IndexLookup.FLAG_PAYLOADS);
					if (fieldObject != null && tokens != null) {
						//logger.info("Processing docID=" + docId + " " + field
						//		+ " for " + value + " , " + tokens);
						if (tokens.iterator().hasNext()) {
							score += tokens.iterator().next().payloadAsFloat(defaultValue);
						}

					}
				}
			}
		} catch (IOException e) {
			//logger.info("Exception in Term Vectors");
			e.printStackTrace();
		}
		return new Double(score);
	}

	@Override
	public Explanation explainScore(int docId, Explanation subQueryScore) {
		Explanation exp = new Explanation();
		double score = score(docId, subQueryScore.getValue());
		exp.setValue(CombineFunction.toFloat(score));
		exp.setDescription(String.format(Locale.ROOT,
				"field value function: (Payload['%s']['%s'])", field, values));
		return exp;
	}

}
