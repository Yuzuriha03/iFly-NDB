本软件旨在将 Fenix 导航数据库转换为 iFly 导航数据库。以下是使用时需注意的要点：  
一、关于航路部分的数据 ：  
    必须具备 NAIP 的 RTE_SEG.csv 文件才能顺利进行转换。软件会将相关航路数据直接添加至 \Community\ifly-aircraft-737max8\Data\navdatapermanent\WPNAVRTE.txt 文件中。若软件无法自动获取该路径，请您手动指定 WPNAVRTE.txt 文件的准确路径。  
二、文件生成路径 ：  
    除了 WPNAVRTE.txt 文件之外，其他所有经转换生成的新文件都将统一存放于 \Community\ifly-aircraft-737max8\Data\navdata\Supplemental 的对应文件夹内。  
三、飞行程序增添遵循 “先 Supplemental 后 Permanent” 原则，即：  
    1、若 Navdata\Supplemental 下有某机场飞行程序，将 Fenix 转换数据与之比对，仅写入iFly数据库中不存在的程序。  
    2、若 Navdata\Supplemental 下无某机场飞行程序，先在 Permanent 文件夹查找对应数据文件。找到则复制到 Supplemental 文件夹对应位置，再按第一种情况逻辑比对写入；未找到则直接在 Supplemental 文件夹对应位置新建相关文件并写入 Fenix 转换数据。

## 许可证 / License

本项目使用 GNU General Public License v3.0 许可证。

本项目使用了 [MSFSLayoutGenerator](https://github.com/HughesMDflyer4/MSFSLayoutGenerator) 工具，该工具基于 MIT 许可证发布。
Copyright (c) 2020 HughesMDflyer4

MSFSLayoutGenerator 的许可证全文如下：

```text
MIT License

Copyright (c) 2020 HughesMDflyer4

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
