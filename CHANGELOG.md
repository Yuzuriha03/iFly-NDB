# Changelog

## v2.1.0 - 2026-08-12

### Added
- 新增 Fenix 数据库与 iFly Permanent 数据集的结构与周期校验，支持在转换前执行只读预检，避免混用不同 AIRAC 周期。

### Fixed
- 修复 iFly 航路/程序映射与固定宽度输出，提升程序文件写入的一致性。
- 修复离场 SID 映射保留问题，避免生成结果丢失原有 iFly SID 关联。
- 规范化跑道记录输出，减少与 iFly 固定宽度格式不匹配的情况。

### Notes
- 本次 release 基于上一个 release `v2.0` 之后的 main 分支提交整理。