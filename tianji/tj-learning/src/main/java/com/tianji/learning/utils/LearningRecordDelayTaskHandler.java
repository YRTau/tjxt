package com.tianji.learning.utils;

import com.tianji.common.utils.JsonUtils;
import com.tianji.common.utils.StringUtils;
import com.tianji.learning.domain.po.LearningLesson;
import com.tianji.learning.domain.po.LearningRecord;
import com.tianji.learning.mapper.LearningRecordMapper;
import com.tianji.learning.service.ILearningLessonService;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.DelayQueue;

@Slf4j
@Component
@RequiredArgsConstructor
public class LearningRecordDelayTaskHandler {

    private final StringRedisTemplate redisTemplate;
    private final LearningRecordMapper recordMapper;
    private final ILearningLessonService lessonService;
    private final DelayQueue<DelayTask<RecordTaskData>> queue = new DelayQueue<>();
    private final static String RECORD_KEY_TEMPLATE = "learning:record:{}";
    /**
     * static ：表示该变量属于类级别，而非实例级别，确保所有实例共享同一控制状态
     * volatile ：确保在多线程环境下的内存可见性，防止线程缓存导致的不一致问题，当一个线程修改begin值时，其他线程能立即看到变化
     */
    private static volatile boolean begin = true;

    @PostConstruct
    public void init(){
        CompletableFuture.runAsync(this::handleDelayTask);
    }
    @PreDestroy
    public void destroy(){
        begin = false;
        log.debug("延迟任务停止执行！");
    }
    /**
     * 处理延迟任务
     * 1. 从延迟队列中取出到期的任务
     * 2. 查询Redis缓存，根据lessonId和sectionId获取学习记录
     * 3. 比较任务数据和缓存数据的moment值
     * 4. 如果moment值一致，说明用户没有继续提交播放进度，是最后一次提交，持久化到数据库
     * 5. 如果moment值不一致，说明用户还在持续提交播放进度，放弃旧数据
     */
    public void handleDelayTask(){
        while (begin) {
            try {
                // 1.获取到期的延迟任务
                DelayTask<RecordTaskData> task = queue.take();
                RecordTaskData data = task.getData();
                // 2.查询Redis缓存
                LearningRecord record = readRecordCache(data.getLessonId(), data.getSectionId());
                if (record == null) {
                    continue;
                }
                // 3.比较数据，moment值
                if(!Objects.equals(data.getMoment(), record.getMoment())) {
                    // 不一致，说明用户还在持续提交播放进度，放弃旧数据
                    continue;
                }

                // 4.一致，持久化播放进度数据到数据库
                // 4.1.更新学习记录的moment
                    //确保不影响更新finished状态，这个是由别的逻辑判断的，
                    //这里只能确认要提交moment，确认要写latestLearnTime，不能更新finished状态
                    //不能判断finished是否完成，有可能是第一次完成以后提交的记录，有可能是未完成的时候提交的记录
                record.setFinished(null);
                recordMapper.updateById(record);
                // 4.2.更新课表最近学习信息
                LearningLesson lesson = new LearningLesson();
                lesson.setId(data.getLessonId());
                lesson.setLatestSectionId(data.getSectionId());
                lesson.setLatestLearnTime(LocalDateTime.now());
                lessonService.updateById(lesson);
            } catch (Exception e) {
                log.error("处理延迟任务发生异常", e);
            }
        }
    }
    /**
     * 添加延迟任务
     * @param record
     */
    public void addLearningRecordTask(LearningRecord record){
        // 1.添加数据到Redis缓存
        writeRecordCache(record);
        // 2.提交延迟任务到延迟队列 DelayQueue
            //延迟时间：20秒
        queue.add(new DelayTask<>(new RecordTaskData(record), Duration.ofSeconds(20)));
    }
    /**
     * 缓存数据结构：
     * Hash结构：
     * key：learning:record:{lessonId}
     * field：sectionId
     * value：LearningRecord
     * 这样每个缓存都是以课程为单位，每个课程下有多个学习记录，可以多命中key，提高查询效率
     * 注意这里key的lessonId是用户+课表id，表示看用户跟课程的中间关联表，所以可以区分不同的用户。（不是单独的courseId）
     */
    public void writeRecordCache(LearningRecord record) {
        log.debug("更新学习记录的缓存数据");
        try {
            // 1.数据转换
            String json = JsonUtils.toJsonStr(new RecordCacheData(record));
            // 2.写入Redis
            String key = StringUtils.format(RECORD_KEY_TEMPLATE, record.getLessonId());
            redisTemplate.opsForHash().put(key, record.getSectionId().toString(), json);
            // 3.添加缓存过期时间
            redisTemplate.expire(key, Duration.ofMinutes(1));
        } catch (Exception e) {
            log.error("更新学习记录缓存异常", e);
        }
    }

    public LearningRecord readRecordCache(Long lessonId, Long sectionId){
        try {
            // 1.读取Redis数据
            String key = StringUtils.format(RECORD_KEY_TEMPLATE, lessonId);
            Object cacheData = redisTemplate.opsForHash().get(key, sectionId.toString());
            if (cacheData == null) {
                return null;
            }
            // 2.数据检查和转换
            return JsonUtils.toBean(cacheData.toString(), LearningRecord.class);
        } catch (Exception e) {
            log.error("缓存读取异常", e);
            return null;
        }
    }

    public void cleanRecordCache(Long lessonId, Long sectionId){
        // 删除数据
        String key = StringUtils.format(RECORD_KEY_TEMPLATE, lessonId);
        redisTemplate.opsForHash().delete(key, sectionId.toString());
    }
    /**
     * 缓存数据结构：
     * Hash结构：
     * key：learning:record:{lessonId}
     * field：sectionId
     * value：RecordCacheData
     * 这样每个缓存都是以课程为单位，每个课程下有多个学习记录，可以多命中key，提高查询效率
     * 注意这里key的lessonId是用户+课表id，表示看用户跟课程的中间关联表，所以可以区分不同的用户。（不是单独的courseId）
     */
    @Data
    @NoArgsConstructor
    private static class RecordCacheData{
        private Long id;
        private Integer moment;
        private Boolean finished;

        public RecordCacheData(LearningRecord record) {
            this.id = record.getId();
            this.moment = record.getMoment();
            this.finished = record.getFinished();
        }
    }
    @Data
    @NoArgsConstructor
    /**
     * 延迟任务数据Data打包成一个类，用于判断是否是最后一次提交，是否要写库了。
     * 1. 包含学习记录的必要信息：lessonId, sectionId, moment
     * 如果sectionId，跟moment都跟上一次提交的一致，说明用户已经不继续更新学习记录了，这是最后一次提交。
     */
    private static class RecordTaskData{
        private Long lessonId;
        private Long sectionId;
        private Integer moment;

        public RecordTaskData(LearningRecord record) {
            this.lessonId = record.getLessonId();
            this.sectionId = record.getSectionId();
            this.moment = record.getMoment();
        }
    }
}
