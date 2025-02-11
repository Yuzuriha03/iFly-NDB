本软件旨在将 Fenix 导航数据库转换为 iFly 导航数据库。以下是使用时需注意的要点：  
一、关于航路部分的数据 ：  
    必须具备 NAIP 的 RTE_SEG.csv 文件才能顺利进行转换。软件会将相关航路数据直接添加至 \Community\ifly-aircraft-737max8\Data\navdatapermanent\WPNAVRTE.txt 文件中。若软件无法自动获取该路径，请您手动指定 WPNAVRTE.txt 文件的准确路径。  
二、文件生成路径 ：  
    除了 WPNAVRTE.txt 文件之外，其他所有经转换生成的新文件都将统一存放于 \Community\ifly-aircraft-737max8\Data\navdata\Supplemental 的对应文件夹内。  
三、飞行程序增添遵循 “先 Supplemental 后 Permanent” 原则，即：  
    1、若 Navdata\Supplemental 下有某机场飞行程序，将 Fenix 转换数据与之比对，仅写入iFly数据库中不存在的程序。  
    2、若 Navdata\Supplemental 下无某机场飞行程序，先在 Permanent 文件夹查找对应数据文件。找到则复制到 Supplemental 文件夹对应位置，再按第一种情况逻辑比对写入；未找到则直接在 Supplemental 文件夹对应位置新建相关文件并写入 Fenix 转换数据。  
