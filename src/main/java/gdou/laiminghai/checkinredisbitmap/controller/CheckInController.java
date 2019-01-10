package gdou.laiminghai.checkinredisbitmap.controller;

import gdou.laiminghai.checkinredisbitmap.service.CheckInService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/checkin")
public class CheckInController {

    @Autowired
    private CheckInService checkInService;

    @PostMapping("/{userId}")
    public String checkIn(@PathVariable(name = "userId") Long userId) {
        checkInService.checkIn(userId);
        return "SUCCESS";
    }

    @GetMapping("/count")
    public Long countDateCheckIn(String date) {
        return checkInService.countDateCheckIn(date);
    }

    @GetMapping("/{userId}")
    public Long countCheckIn(@PathVariable(name =  "userId") Long userId,
                             @RequestParam(name = "startDate", required = true) String startDate,
                             @RequestParam(name = "endDate", required = true) String endDate) {
        return checkInService.countCheckIn(userId, startDate, endDate);
    }

    @GetMapping("/continuousdays/{userId}")
    public long getContinuousCheckIn(@PathVariable(name = "userId") Long userId) {
        return checkInService.getContinuousCheckIn(userId);
    }
}
