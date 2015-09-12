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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.search.function.ScoreFunction;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryParsingException;
import org.elasticsearch.index.query.functionscore.ScoreFunctionParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Parses out a function_score function that looks like:
 *
 * <pre>
 *     {
 *         "payload_factor": {
 *             "field": "myField",
 *             "value": "myValue,
 *             "modifier": "square",
 *             "null_value": 0
 *         }
 *     }
 * </pre>
 */
public class PayloadScoringFunctionParser implements ScoreFunctionParser {
	public static String[] NAMES = { "payload_factor", "payloadFactor" };
	//private ESLogger logger = Loggers
	//		.getLogger(PayloadScoringFunctionParser.class);


	@Override
	public ScoreFunction parse(QueryParseContext parseContext,
			XContentParser parser) throws IOException, QueryParsingException {
		String currentFieldName = null;
		String field = null;
		List<String> values = new ArrayList<String>();
		Float defaultValue = 0f;
		XContentParser.Token token;

		while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
			//logger.info("Token type is " + token);
			if (token == XContentParser.Token.FIELD_NAME) {
				currentFieldName = parser.currentName();
			} else if (token.isValue()) {
				if ("field".equals(currentFieldName)) {
					field = parser.text();
				} else if ("defaultValue".equals(currentFieldName)) {
					defaultValue = parser.floatValue();
				} else {
					throw new QueryParsingException(parseContext.index(),
							NAMES[0] + " query does not support ["
									+ currentFieldName + "][" + parser.text() + "]" );
				}
			} else if (token == XContentParser.Token.START_ARRAY
					&& currentFieldName == "values") {
				while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
					Object value = parser.objectBytes();
					if (value == null) {
						throw new QueryParsingException(parseContext.index(),
								"No value specified for terms query");
					}
					values.add(parser.text());
				}
			} else if ("factor".equals(currentFieldName)
					&& (token == XContentParser.Token.START_ARRAY || token == XContentParser.Token.START_OBJECT)) {
				throw new QueryParsingException(parseContext.index(), "["
						+ NAMES[0]
						+ "] field 'factor' does not support lists or objects");
			}
		}

		if (field == null) {
			throw new QueryParsingException(parseContext.index(), "["
					+ NAMES[0] + "] required field 'field' missing");
		}
		return new PayloadScoringFunction(parseContext.lookup()
				.indexLookup(), field, values, defaultValue);
	}

	@Override
	public String[] getNames() {
		return NAMES;
	}
}
