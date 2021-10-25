package com.sefaz.batchtests;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.support.ListItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import java.io.IOException;

@Configuration
@EnableBatchProcessing
public class SpringBatchConfiguration {

    private static final String[] TOKENS = {"bookname", "bookauthor", "bookformat", "isbn", "publishyear"};

    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Bean
    @StepScope
    public FlatFileItemReader<BookRecord> csvItemReader(@Value("#{jobParameters['file.input']}") String input){
        FlatFileItemReaderBuilder<BookRecord> builder = new FlatFileItemReaderBuilder<>();
        FieldSetMapper<BookRecord> bookRecordFieldSetMapper = new BookRecordFieldSetMapper();
        return builder
                .name("bookRecordItemReader")
                .resource(new FileSystemResource(input))
                .delimited()
                .names(TOKENS)
                .fieldSetMapper(bookRecordFieldSetMapper)
                .build();
    }

    @Bean
    @StepScope
    public JsonFileItemWriter<Book> jsonItemWriter(@Value("#{jobParameters['file.input']}") String output) throws IOException {
        JsonFileItemWriterBuilder<Book> builder = new JsonFileItemWriterBuilder<>();
        JacksonJsonObjectMarshaller<Book> marshaller = new JacksonJsonObjectMarshaller<>();
        return builder
                .name("bookItemWriter")
                .jsonObjectMarshaller(marshaller)
                .resource(new FileSystemResource(output))
                .build();
    }

    @Bean
    @StepScope
    public ListItemWriter<BookDetails> listItemWriter(){
        return new ListItemWriter<BookDetails>();
    }

    @Bean
    @StepScope
    public BookItemProcessor bookItemProcessor(){
        return new BookItemProcessor();
    }

    @Bean
    @StepScope
    public BookDetailsItemProcessor bookDetailsItemProcessor(){
        return new BookDetailsItemProcessor();
    }

    @Bean
    public Step step1(ItemReader<BookRecord> csvItemReader, ItemWriter<Book> jsonItemWriter) throws Exception {
        return stepBuilderFactory
                .get("step1")
                .<BookRecord, Book> chunk(3)
                .reader(csvItemReader)
                .writer(jsonItemWriter)
                .build();
    }

    @Bean
    public Step step2(ItemReader<BookRecord> csvItemReader, ItemWriter<BookDetails> listItemWriter){
        return stepBuilderFactory
                .get("step2")
                .<BookRecord, BookDetails> chunk(3)
                .reader(csvItemReader)
                .processor(bookDetailsItemProcessor())
                .writer(listItemWriter)
                .build();
    }

    @Bean
    public Job transformBookRecords(Step step1, Step step2){
        return jobBuilderFactory
                .get("transformBookRecords")
                .flow(step1)
                .next(step2)
                .end()
                .build();
    }

}
