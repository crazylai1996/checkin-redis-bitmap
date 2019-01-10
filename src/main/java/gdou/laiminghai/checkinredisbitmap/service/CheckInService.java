package gdou.laiminghai.checkinredisbitmap.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Service
@Slf4j
public class CheckInService {

    private static final String CHECK_IN_PRE_KEY = "USER_CHECK_IN::DAY::";

    private static final String CONTINUOUS_CHECK_IN_COUNT_PRE_KEY = "USER_CHECK_IN::CONTINUOUS_COUNT::";

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 用户签到
     *
     * @param userId 用户ID
     */
    public void checkIn(Long userId) {
        String today = LocalDate.now().format(DATE_TIME_FORMATTER);
        if(isCheckIn(userId, today))
            return;
        stringRedisTemplate.opsForValue().setBit(getCheckInKey(today), userId, true);
        updateContinuousCheckIn(userId);
    }

    /**
     * 检查用户是否签到
     *
     * @param userId
     * @param date
     * @return
     */
    public boolean isCheckIn(Long userId, String date) {
        Boolean isCheckIn = stringRedisTemplate.opsForValue().getBit(getCheckInKey(date), userId);
        return Optional.ofNullable(isCheckIn).orElse(false);
    }

    /**
     * 统计特定日期签到总人数
     *
     * @param date
     * @return
     */
    public Long countDateCheckIn(String date) {
        byte [] key = getCheckInKey(date).getBytes();
        Long result = stringRedisTemplate.execute(new RedisCallback<Long>() {
            @Nullable
            @Override
            public Long doInRedis(RedisConnection connection) throws DataAccessException {
                return connection.bitCount(key);
            }
        });
        return Optional.ofNullable(result).orElse(0L);
    }

    /**
     * 获取用户某个时间段签到次数
     *
     * @param userId
     * @param startDate
     * @param endDate
     * @return
     */
    public Long countCheckIn(Long userId, String startDate, String endDate) {
        LocalDate startLocalDate = LocalDate.parse(startDate, DATE_TIME_FORMATTER);
        LocalDate endLocalDate = LocalDate.parse(endDate, DATE_TIME_FORMATTER);
        AtomicLong count = new AtomicLong(0);
        long distance = Period.between(startLocalDate, endLocalDate).get(ChronoUnit.DAYS);
        if(distance < 0)
            return count.get();
        Stream.iterate(startLocalDate, d -> d.plusDays(1)).limit(distance + 1).forEach((LocalDate date) -> {
            Boolean isCheckIn = stringRedisTemplate.opsForValue().
                    getBit(getCheckInKey(date.format(DATE_TIME_FORMATTER)), userId);
            if(isCheckIn)
                count.incrementAndGet();
        });
        return count.get();
    }

    /**
     * 更新用户连续签到天数：+1
     * @param userId
     */
    public void updateContinuousCheckIn(Long userId) {
        String key = getContinuousCheckInKey(userId);
        String val = stringRedisTemplate.opsForValue().get(key);
        long count = 0;
        if(val != null){
            count = Long.parseLong(val);
        }
        count ++;
        stringRedisTemplate.opsForValue().set(key, String.valueOf(count));
        //设置第二天过期
        stringRedisTemplate.execute(new RedisCallback<Void>() {
            @Nullable
            @Override
            public Void doInRedis(RedisConnection connection) throws DataAccessException {
                LocalDateTime dateTime = LocalDateTime.now().plusDays(2).withHour(0).withMinute(0).withSecond(0);
                connection.expireAt(key.getBytes(), dateTime.toInstant(ZoneOffset.of("+8")).getEpochSecond());
                return null;
            }
        });
    }

    /**
     * 获取用户连续签到天数
     * @param userId
     * @return
     */
    public Long getContinuousCheckIn(Long userId) {
        String key = getContinuousCheckInKey(userId);
        String val = stringRedisTemplate.opsForValue().get(key);
        if(val == null){
            return 0L;
        }
        return Long.parseLong(val);
    }

    private String getCheckInKey(String date) {
        return CHECK_IN_PRE_KEY + date;
    }

    private String getContinuousCheckInKey(Long userId) {
        return CONTINUOUS_CHECK_IN_COUNT_PRE_KEY + userId;
    }
}
