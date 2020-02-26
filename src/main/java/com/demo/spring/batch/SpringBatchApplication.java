package com.demo.spring.batch;

import com.demo.spring.batch.model.Player;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.FileSystemResource;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;

@SpringBootApplication
@EnableBatchProcessing
@Data
@NoArgsConstructor
@AllArgsConstructor
@EnableRetry
public class SpringBatchApplication implements CommandLineRunner {

	@Autowired
	private JobBuilderFactory jobs;

	@Autowired
	private StepBuilderFactory steps;

	@Autowired
	JobLauncher jobLauncher;

	public static void main(final String[] args) {
		SpringApplication.run(SpringBatchApplication.class, args);
	}

	@Bean
	public Job job() throws Exception {
		return jobs.get("myJob").start(step1()).build();
	}

	@Bean
	public Step step1() throws Exception {
		return steps.get("step1").<Player, Player>chunk(1).reader(loadRecordsReader())
				.processor(myProcessor()).writer(myWriter())
				.build();
	}

//	@Bean
//	@StepScope
//	public MyReader myReader() {
//		return new MyReader();
//	}

	@Bean
	public MyProcessor myProcessor() {
		return new MyProcessor();
	}

	@Bean
	public MyWriter myWriter() {
		return new MyWriter();
	}


	@Override
	public void run(String... args) throws Exception {
		JobParameters params = new JobParametersBuilder()
				.addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();
		jobLauncher.run(job(), params);
	}

	int counter;

	@Bean
	@StepScope
	@Retryable(include = { ItemStreamException.class }, maxAttempts = 5)
	ItemReader<Player> loadRecordsReader() throws Exception {

		String filePath = "src/main/resources/player1_s.csv";
		if (++counter > 3) {
			filePath = "src/main/resources/players.csv";
		}
		System.out.println("Loading records from "  + filePath + " try " + counter);

		FlatFileItemReader<Player> itemReader = new FlatFileItemReader<>();
		itemReader.setResource(new FileSystemResource(filePath));
		itemReader.setLinesToSkip(1);
		//DelimitedLineTokenizer defaults to comma as its delimiter
		DefaultLineMapper<Player> lineMapper = new DefaultLineMapper<>();
		lineMapper.setLineTokenizer(new DelimitedLineTokenizer());
		lineMapper.setFieldSetMapper(new PlayerFieldSetMapper());
		itemReader.setLineMapper(lineMapper);
		itemReader.open(new ExecutionContext());
		return itemReader;
	}

	protected static class PlayerFieldSetMapper implements FieldSetMapper<Player> {
		public Player mapFieldSet(FieldSet fieldSet) {
			Player player = new Player();

			player.setID(fieldSet.readString(0));
			player.setLastName(fieldSet.readString(1));
			player.setFirstName(fieldSet.readString(2));
			player.setPosition(fieldSet.readString(3));
			player.setBirthYear(fieldSet.readInt(4));
			player.setDebutYear(fieldSet.readInt(5));

			return player;
		}
	}

}
