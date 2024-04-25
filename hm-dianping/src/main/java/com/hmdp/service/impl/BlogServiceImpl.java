package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    @Autowired
    private IUserService userService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private IFollowService followService;

    /**
     * 保存blog
     * @param blog
     * @return
     */
    @Override
    public Result saveBlog(Blog blog) {
        //1、获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        //2、保存探店博文
        boolean isSuccess = save(blog);
        if(!isSuccess){
            return Result.fail("新增blog失败！");
        }
        //3、获取blog作者的所有粉丝id
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        //4、推送笔记id给所有粉丝
        for (Follow follow : follows) {
            Long userId = follow.getUserId();
            String key = RedisConstants.FEED_KEY+userId;
            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());
        }
        //5、blog返回id
        return Result.ok(blog.getId());
    }

    /**
     * 修改点赞数量
     * @param id
     * @return
     */
    @Override
    public Result likeBlog(Long id) {
        //1、获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2、判断当前用户是否已点赞
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        if(score == null){
            //3、未点赞，则可以点赞
            //3.1、数据库字段+1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            //3.2、将用户id添加到zset中
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().add(key,userId.toString(),System.currentTimeMillis());
            }
        }else{
            //4、已点赞，则取消点赞
            //4.1、数据库字段-1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            //4.2、从zset中将用户id移除
            if (isSuccess) {
                stringRedisTemplate.opsForZSet().remove(key,userId.toString());
            }
        }
        return Result.ok();
    }

    /**
     * 获取我的blog
     * @param current
     * @return
     */
    @Override
    public Result queryMyBlog(Integer current) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        // 根据用户查询
        Page<Blog> page = query().eq("user_id", user.getId()).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    /**
     * 根据id查询博主的探店笔记
     * @param current
     * @param id
     * @return
     */
    @Override
    public Result queryBlogByUserId(Integer current, Long id) {
        // 根据用户查询
        Page<Blog> page = query().eq("user_id", id).page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    /**
     * 查询热点blog
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    /**
     * 根据id查询blog和对应用户信息
     * @param id
     * @return
     */
    @Override
    public Result queryBlogById(Long id) {
        //1、查询blog信息
        Blog blog = getById(id);
        if (blog == null) {
            return Result.fail("blog不存在！");
        }
        //2、封装用户信息
        queryBlogUser(blog);
        //3、获取点赞信息
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    /**
     * 查询blog点赞排行榜top5
     * @param id
     * @return
     */
    @Override
    public Result queryBlogLike(Long id) {
        //1、从redis查询top5用户id
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(top5 == null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        //2、封装返回结果为UserDto
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idsStr = StrUtil.join(",", ids);
        List<UserDTO> userDTOS = userService
                .query().in("id",ids).last("order by field(id,"+idsStr+")").list()  //自定义结果的id顺序
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOS);
    }

    /**
     * 关注列表滚动分页查询
     * @param max
     * @param offset
     * @return
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        //1、获取当前用户
        Long userId = UserHolder.getUser().getId();
        //2、滚动分页查询,获取用户收件箱中blog的id集合
        String key =RedisConstants.FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet().
                        reverseRangeByScoreWithScores(key, 0, max, offset, SystemConstants.FEED_PAGE_SIZE);
        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }

        //3、解析数据，获取blog_ids,minTime（时间戳）,offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime =0l;
        int os=1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            //blog_id
            ids.add(Long.valueOf(typedTuple.getValue()));
            //time
            long score = typedTuple.getScore().longValue();
            if(score == minTime){
                os++;
            }else{
                minTime = score;
                os=1;
            }
        }
        os = minTime==max? os+offset:os;
        //4、获取blogs
        String idsStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("order by field(id," + idsStr + ")").list();
        for (Blog blog : blogs) {
            //4.1、封装用户信息
            queryBlogUser(blog);
            //4.1、获取点赞信息
            isBlogLiked(blog);
        }
        //5、返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(os);
        return Result.ok(scrollResult);
    }

    private void isBlogLiked(Blog blog) {
        UserDTO user = UserHolder.getUser();
        if(user == null){
            //用户未登录
            return;
        }
        //1、获取当前用户
        Long userId = user.getId();
        //2、判断当前用户是否已点赞
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    private void queryBlogUser(Blog blog){
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
