<h2>基于etcd分布式定时任务框架</h2>

&emsp;&emsp;在开发过程中，往往需要系统执行一些定时的任务，例如我们需要将数据进行迁移，又或者需要做一些数据的离线统计工作，这些都需要定时任务来进行处理。传统的方法就是quartz来写个定时任务，然后该机器就会在特定时间执行我们要执行的代码，但是假如这台机器出现故障，那么这个定时任务就不会执行。 
　　在集群环境中，我们希望即使在某台机器出现故障，那么其他机器就可以将任务接管过来，继续执行任务。 
项目github:https://github.com/Mrfogg/gojob
项目github:[https://github.com/Mrfogg/gojob](https://github.com/Mrfogg/gojob)
<h2>使用方式</h2>
&emsp;&emsp;见test_test.go 如果需要在自己的机器上测试分布式，只需要指定不同的nodename即可。（确保你的机器已经部署了etcd）
