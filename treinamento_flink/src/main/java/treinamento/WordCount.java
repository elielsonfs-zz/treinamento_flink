/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package treinamento;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

/**
 * Implements the "WordCount" program that computes a simple word occurrence
 * histogram over text files.
 */
@SuppressWarnings("serial")
public class WordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);
		
		// Seta o ambiente de execução
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Seta os parametros para serem acessados pela interface web
		env.getConfig().setGlobalJobParameters(params);

		// Declara um data set de texto
		DataSet<String> text;
		
		// Lê o arquivo de entrada
		text = env.readTextFile(params.get("input"));

		DataSet<Tuple2<String, Integer>> counts =
				// Gera uma lista de tuplas no padrão: (palavra, quantidade);
				text.flatMap(new Tokenizer())
						// Realiza o agrupamento da posição "0" e soma o total do campo 1"
						.groupBy(0).sum(1);

		// Grava o resultado em arquivo CSV
		if (params.has("output")) {
			
			Properties properties = new Properties();
			properties.setProperty("bootstrap.servers", "localhost:9092");
			// only required for Kafka 0.8
			properties.setProperty("zookeeper.connect", "localhost:2181");
			properties.setProperty("group.id", "test");
			
			counts.writeAsCsv(params.get("output"), "\n", " ");
			// Finaliza a execução
			env.execute("WordCount Example");
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}
		
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// Retira caracteres e realiza o Split
			String[] tokens = value.toLowerCase().split("\\W+");

			// Gera os pares
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
	

}