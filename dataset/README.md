## 开发步骤
复制`core-site.xml`与`hbase-site.xml`两个文件到`resources`文件夹

## 打包方式 
方式1
```
gradle clean build -xtest
# 优点：打包出来的包很小,只有几十k
# 缺点：需要把依赖的build/libs/lib/*.jar复制到flink环境中
```
方式2
```
gradle clean shadowJar -xtest
# 优点：打包出来的项目可独立运行,依赖包都在,不需要将依赖放到flink环境中
# 缺点：打包时间长,项目庞大
```