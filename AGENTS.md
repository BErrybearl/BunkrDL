# 协作约定

- 提交格式：`agent(BunkrDL): 中文标题`；按路径提交，不用 `git add -A`。
- 测试：`BUNKRDL_HOME=<临时目录> python -m unittest discover -s tests`（离线，不联网）。测试一律用临时 `BUNKRDL_HOME`，不得触碰真实状态库和下载目录。
- 隐私底线：不提交个人路径、账号、token、真实配置或下载内容；不绕过人机验证。
- 用语统一：“下载链接”“链接已生成”“检查外部下载文件夹”。
