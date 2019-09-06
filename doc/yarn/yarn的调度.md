# yarn的调度
## FIFO scheduler(先进先出)
- 先进先出
- 集群有大任务一致在运行时新提交的任务会一直等待
## capacity scheduler(容量调度器)
- 配置
    - yarn-site.xml:指定调度器
    - capacity-scheduler.xml:配置队列

主要配置:
1. 队列名
2. 队列资源
3. 弹性队列(最大占用资源)

层次关系
```
root
|----prod
|----dev
   |----science
   |----eng
```
yarn-site.xml
```xml
<configuration>
    <property>
        <name>yarn.resourcemanager.scheduler.class</name>
        <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
    </property>
</configuration>
```
capacity-scheduler.xml
```xml
<configuration>
    <property>
        <name>yarn.scheduler.capacity.root.queues</name>
        <value>prod,dev</value>
        <description>
          The queues at the this level (root is the root queue).
        </description>
    </property>
    <!-- 定义prod队列下的子队列 -->
    <property>
        <name>yarn.scheduler.capacity.root.dev.queues</name>
        <value>science,eng</value>
        <description>
          The queues at the this level (root is the root queue).
        </description>
    </property>
    <!-- prod占用40%的资源,未配置最大资源，当dev队列空闲时可全部占用 -->
    <property>
        <name>yarn.scheduler.capacity.root.prod.capacity</name>
        <value>40</value>
    </property>
    
    <!-- dev占用60%的资源,最大占用到75% -->
    <property>
        <name>yarn.scheduler.capacity.root.dev.capacity</name>
        <value>60</value>
    </property>
    <property>
        <name>yarn.scheduler.capacity.root.dev.maximum-capacity</name>
        <value>75</value>
    </property>
    
    <!-- dev下的science和eng队列各占50% -->
    <property>
        <name>yarn.scheduler.capacity.root.dev.science.capacity</name>
        <value>50</value>
    </property>
    <property>
        <name>yarn.scheduler.capacity.root.dev.eng.capacity</name>
        <value>50</value>
    </property>
</configuration>
```
> 作业提交指定队列的时候，队列名直接是后缀名，例如eng，而不是root.dev.eng
## fair scheduler(公平调度器)
- 配置
    - yarn-site.xml:指定调度器
    - fair-allocation.xml:配置队列
    
yarn-site.xml
```xml
<configuration>
    <property>
         <name>yarn.resourcemanager.scheduler.class</name> 
         <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value> 
    </property>
    <property>
         <name>yarn.scheduler.fair.allocation.file</name> 
         <value>$HADOOP_HOME/etc/hadoop/fair-allocation.xml</value> 
    </property>
    <!-- 是否开启抢占 -->
    <property> 
         <name>yarn.scheduler.fair.preemption</name> 
         <value>false</value> 
    </property>
</configuration>
```
fair-allocation.xml
```xml
<allocations>
    <!-- 队列默认策略为fair -->
    <defaultQueueSchedulingPolicy>fair</defaultQueueSchedulingPolicy>
    <!-- prod队列采用先进先出，占用40%的资源 -->
    <queue name="prod">
        <weight>40</weight>
        <schedulingPolicy>fifo</schedulingPolicy>
        <!-- 配置最大最小资源数量和最大运行任务的数量 -->
        <minResources>10000mb,10vcores</minResources> 
        <maxResources>90000mb,100vcores</maxResources> 
        <maxRunningApps>50</maxRunningApps>
        <!--抢占资源的配置
        基于最小资源的配置，单位:秒，也可配置全局的defaultMinSharePreemptionTimeout
        <minSharePreemptionTimeout>3</minSharePreemptionTimeout>
        基于共享份额的配置，默认值0.5，如果3秒未获得份额的0.5则抢占
        <fairSharePreemptionTimeout>3</fairSharePreemptionTimeout>
        <fairSharePreemptionThreshold>0.5</fairSharePreemptionThreshold>
        -->
    </queue>
    <!-- dev里面有两个队列，默认使用fair策略 -->
    <queue name="dev">
        <weight>60</weight>
        <queue name="eng" />
        <queue name="science" />
    </queue>
    <queuePlacementPolicy>
        <rule name="specified" create="false" />
        <rule name="primaryGroup" create="false" />
        <rule name="default" queue="dev.eng" />
    </queuePlacementPolicy>
    <!--如果不指定，默认的规则
    如果指定的queue不存在，则创建用户名相同的queue
    <queuePlacementPolicy>
        <rule name="specified" />
        <rule name="user" />
    </queuePlacementPolicy>
    -->
</allocations>
```
基于unix用户动态创建queue的配置
https://www.jianshu.com/p/5c420c138544