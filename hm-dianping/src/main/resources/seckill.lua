-- 优惠券id
local voucherId = ARGV[1]
-- 用户id
local userId = ARGV[2]
-- 订单id
local orderId = ARGV[3]

-- 库存key
local stockKey = "seckill:stock:"..voucherId
-- 订单key
local orderKey = "seckill:order:"..voucherId

-- 判断库存是否充足
if(tonumber(redis.call("get",stockKey))<=0)then
  return 1
end
-- 判断用户是否下过订单
if(redis.call("sismember",orderKey,userId)==1)then
  return 2
end

-- 扣减库存
redis.call("incrby",stockKey,-1)
-- 添加用户
redis.call("sadd",orderKey,userId)
-- 添加订单信息到消息队列
redis.call("xadd","stream.orders","*","id",orderId,"voucherId",voucherId,"userId",userId)
return 0