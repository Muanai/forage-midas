package com.jpmc.midascore.controller;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Balance;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Optional;

@RestController
public class BalanceController {

    private final UserRepository userRepository;

    public BalanceController(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @GetMapping("/balance")
    public Balance getBalance(@RequestParam Long userId) {
        Object userObj = userRepository.findById(userId);
        UserRecord user = null;

        if (userObj instanceof Optional) {
            user = ((Optional<UserRecord>) userObj).orElse(null);
        } else if (userObj instanceof UserRecord) {
            user = (UserRecord) userObj;
        }

        if (user != null) {
            return new Balance(user.getBalance());
        }

        return new Balance(0);
    }
}