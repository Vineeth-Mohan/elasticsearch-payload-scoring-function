package com.factweavers.elasticsearch.plugin;

import org.elasticsearch.index.query.functionscore.FunctionScoreModule;
import org.elasticsearch.plugins.AbstractPlugin;

import com.factweavers.elasticsearch.payloadscorefunction.PayloadScoringFunctionParser;

public class PayloadScoringPlugin extends AbstractPlugin {

	@Override
	public String name() {
		return "PayloadScoring";
	}

	@Override
	public String description() {
		return "A scoring function plugin based on payload for function_score query";
	}

	public void onModule(FunctionScoreModule module) {
		module.registerParser(PayloadScoringFunctionParser.class);
	}


}
