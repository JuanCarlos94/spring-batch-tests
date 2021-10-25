package com.sefaz.batchtests;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.PropertySource;

@SpringBootApplication
@PropertySource("classpath:application.properties")
public class BatchTestsApplication implements CommandLineRunner {


    @Autowired
    public JobLauncher jobLauncher;

    @Autowired
    private Job transformBooksRecordsJob;

    @Value("${file.input}")
    private String input;

    @Value("${file.output}")
    private String output;

    public static void main(String[] args) {
        SpringApplication.run(BatchTestsApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString("file.input", input);
        jobParametersBuilder.addString("file.output", output);
        jobLauncher.run(transformBooksRecordsJob, jobParametersBuilder.toJobParameters());
    }
}
