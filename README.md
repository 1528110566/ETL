# ETL
  本项目是为ETL工具打造，在使用ETL工具时发现有很多不便，所以采用Java外部调用的方式实现ETL中的存储过程、函数等的调用且保持原有ETL工具的功能。
  本作品是ojdbc包的深度适配，主要用于外部调用Oracle中的存储过程、函数、自动拼装SQL语句、动态切换schema等功能。为了实现调用时的规范，代码中进行了深层次的限制，采用了大量的Exception保证程序的正常运行。
  本作品还实现了Oracle11g中的部分内置函数，除了日常调用外，还能掌握内置函数的实现原理。
  本作品可拓展性高，除了使用适配好的方法外，还可以发送自己拼接好的SQL语句，使程序运行更加平稳。
  采用向后兼容的编程风格，使得在后续新增功能时可以无需修改前方已存在的代码块。
  程序中存在全局静态常量，用于设置登录时的URL、用户名及密码，修改时方便，且程序采用单例模式保证在运行时只会进行一次数据库连接，减少资源消耗与Oracle 的session消耗。

  在运行前需要在system.constant.Main类中指定Oracle数据库所在的URL、用户名、密码及当前的schema，在test包中存在大量的测试以供检测程序运行的稳定性。


# 将源代码放出以供测试与参考
