Запуск программы: 

python log_parser.py --log=file|directory

Указывается относительный путь к файлу логов, либо к директории с лог-файлами веб-сервера.

Формат файлов не проверяется, считаем, что в указанной директории находятся только лог-файлы формата

%h - - %t "%r" %s %b "%{Referer}" "%{User-Agent}" %d

Если на входе указывается директория, то обрабатываются все имеющиеся там файлы. 

При обработке создается sqlite база (файл sqlite-db/db_logs.db), в которой содержится столько таблиц, сколько лог-файлов было обработано за выполнение скрипта.
При этом таблицы названы соответственно именам файлов.
Из каждой из этих таблиц выбираются следующие данные:
- общее количество выполненных запросов
- количество запросов по HTTP-методам: GET, POST, PUT, DELETE, OPTIONS, HEAD. 
- топ 3 IP адресов, с которых было сделано наибольшее количество запросов
- топ 3 самых долгих запросов

Результат сохраняется в виде json файла с соответствующим названием в директории res. 