package io.rcardin.spring.kafka.stream;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Service;

import javax.persistence.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean
	public MessageConverter jsonMessageConverter() {
		return new Jackson2JsonMessageConverter();
	}

	@Bean
	public Queue wordsQueue() {
		return new Queue("words", false);
	}

	@Bean
	public Queue wordCountersQueue() {
		return new Queue("word-counters", false);
	}

	@Bean
	public TopicExchange exchange() {
		return new TopicExchange("app.topic");
	}

	@Bean
	public Binding binding(Queue wordsQueue, TopicExchange exchange) {
		return BindingBuilder.bind(wordsQueue).to(exchange).with("words");
	}

	@Bean
	public Binding binding2(Queue wordCountersQueue, TopicExchange exchange) {
		return BindingBuilder.bind(wordCountersQueue).to(exchange).with("word-counters");
	}
}

@Service
class WordCountService {

	private final RabbitTemplate rabbitTemplate;
	private final WordRepository wordRepository;
	private final AtomicLong counter = new AtomicLong(0);

	public WordCountService(RabbitTemplate rabbitTemplate, WordRepository wordRepository) {
		this.rabbitTemplate = rabbitTemplate;
		this.wordRepository = wordRepository;
	}

	public void processWord(String word) {
		List<WordEntity> words = wordRepository.findByValue(word);
		if (words.isEmpty()) {
			WordEntity entity = new WordEntity();
			entity.setValue(word);
			entity.setCount(1L);
			wordRepository.save(entity);
		} else {
			WordEntity entity = words.get(0);
			entity.setCount(entity.getCount() + 1);
			wordRepository.save(entity);
		}
		rabbitTemplate.convertAndSend("app.topic", "word-counters", word);
	}
}

@Entity
@Table(name = "words")
class WordEntity {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(unique = true)
	private String value;

	private Long count = 0L;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Long getCount() {
		return count;
	}

	public void setCount(Long count) {
		this.count = count;
	}
}

interface WordRepository extends JpaRepository<WordEntity, Long> {
	List<WordEntity> findByValue(String value);
}