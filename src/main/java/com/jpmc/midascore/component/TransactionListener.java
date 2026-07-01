package com.jpmc.midascore.component;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@Component
public class TransactionListener {

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
        this.restTemplate = new RestTemplate();
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(Transaction transaction) {
        Object senderObj = userRepository.findById(transaction.getSenderId());
        Object recipientObj = userRepository.findById(transaction.getRecipientId());

        UserRecord sender = null;
        UserRecord recipient = null;

        if (senderObj instanceof Optional) {
            sender = ((Optional<UserRecord>) senderObj).orElse(null);
        } else if (senderObj instanceof UserRecord) {
            sender = (UserRecord) senderObj;
        }

        if (recipientObj instanceof Optional) {
            recipient = ((Optional<UserRecord>) recipientObj).orElse(null);
        } else if (recipientObj instanceof UserRecord) {
            recipient = (UserRecord) recipientObj;
        }

        if (sender != null && recipient != null && sender.getBalance() >= transaction.getAmount()) {
            float incentiveAmount = 0f;
            try {
                Incentive response = restTemplate.postForObject("http://localhost:8080/incentive", transaction, Incentive.class);
                if (response != null) {
                    incentiveAmount = response.getAmount();
                }
            } catch (Exception e) {
                incentiveAmount = 0f;
            }

            sender.setBalance(sender.getBalance() - transaction.getAmount());
            recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

            userRepository.save(sender);
            userRepository.save(recipient);

            TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
            transactionRecordRepository.save(record);

            if ("wilbur".equals(sender.getName())) {
                System.out.println("WILBUR-BALANCE: " + (int) Math.floor(sender.getBalance()));
            }
            if ("wilbur".equals(recipient.getName())) {
                System.out.println("WILBUR-BALANCE: " + (int) Math.floor(recipient.getBalance()));
            }
        }
    }
}