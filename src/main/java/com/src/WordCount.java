package com.src;


import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

public class WordCount {
    public interface WordCountOptions extends PipelineOptions {
        @Description("Path of the input file")
        @Required
        String getInputFile();
        void setInputFile(String value);

        @Description("Path of the output prefix")
        @Required
        String getOutputPrefix();
        void setOutputPrefix(String value);
    }

    public static void main(String[] args) {

        WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply("ExtractWords", FlatMapElements.into(TypeDescriptors.strings())
                        .via((String line) -> Arrays.asList(line.split("\\W+"))))
                .apply("CountWords", Count.perElement())
                .apply("FormatResults", MapElements.into(TypeDescriptors.strings())
                        .via((KV<String, Long> wordCount) -> wordCount.getKey() + ": " + wordCount.getValue()))
                .apply("WriteResults", TextIO.write().to(options.getOutputPrefix()).withSuffix(".txt").withoutSharding());

        pipeline.run().waitUntilFinish();
    }
}
