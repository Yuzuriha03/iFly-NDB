本软件旨在将 Fenix 导航数据库转换为 iFly 导航数据库。以下是使用时需注意的要点：  
一、关于航路部分的数据 ：  
    必须具备 NAIP 的 RTE_SEG.csv 文件才能顺利进行转换。软件会将相关航路数据直接添加至 \Community\ifly-aircraft-737max8\Data\navdatapermanent\WPNAVRTE.txt 文件中。若软件无法自动获取该路径，请您手动指定 WPNAVRTE.txt 文件的准确路径。  
二、文件生成路径 ：  
    除了 WPNAVRTE.txt 文件之外，其他所有经转换生成的新文件都将统一存放于 \Community\ifly-aircraft-737max8\Data\navdata\Supplemental 的对应文件夹内。  
三、本软件遵循 “先 Supplemental 后 Permanent” 的飞行程序增添原则，具体逻辑如下：  
    情况一 ：当 iFly 数据库的 Navdata\Supplemental 文件夹中已存在某机场的飞行程序时，软件会将自 Fenix 转换而来的飞行程序数据与该文件夹内已有的飞行程序数据进行比对，仅将 iFly 数据库中尚未包含的飞行程序写入相关文件。  
    情况二 ：若 iFly 数据库的 Navdata\Supplemental 文件夹中不存在某机场的飞行程序，软件会先尝试在 Permanent 文件夹中查找该机场对应的数据文件。若在 Permanent 文件夹中找到，则将其复制到 Supplemental 文件夹的对应位置，随后按照情况一的逻辑进行数据比对和写入操作；若在 Permanent 文件夹中不存在相应数据文件，软件将直接在 Supplemental 文件夹的对应位置新建相关文件，并将自 Fenix 转换而来的飞行程序数据写入其中。  