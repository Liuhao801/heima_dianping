package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

    /**
     * 根据id查询blog和对应用户信息
     * @param id
     * @return
     */
    Result queryBlogById(Long id);

    /**
     * 查询热点blog
     * @param current
     * @return
     */
    Result queryHotBlog(Integer current);

    /**
     * 保存blog
     * @param blog
     * @return
     */
    Result saveBlog(Blog blog);

    /**
     * 修改点赞数量
     * @param id
     * @return
     */
    Result likeBlog(Long id);

    /**
     * 获取我的blog
     * @param current
     * @return
     */
    Result queryMyBlog(Integer current);

    /**
     * 查询blog点赞排行榜top5
     * @param id
     * @return
     */
    Result queryBlogLike(Long id);

    /**
     * 根据id查询博主的探店笔记
     * @param current
     * @param id
     * @return
     */
    Result queryBlogByUserId(Integer current, Long id);

    /**
     * 关注列表滚动分页查询
     * @param max
     * @param offset
     * @return
     */
    Result queryBlogOfFollow(Long max, Integer offset);
}
