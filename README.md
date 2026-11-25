# SWE 307 — Big Data Proje 3

Ubuntu’da Java 17 ile çalışan bir personel harcama uygulaması. POS kayıtları MySQL’deki personel-departman eşleşmesine göre Kafka’ya gönderilir. Spark Structured Streaming geçerli harcama kayıtlarını Cassandra’ya yazar. Tarayıcıdan bir personel seçilince Spring Boot profil bilgilerini MySQL’den, fotoğrafı HDFS’den alır ve toplam harcamayı Spark’a hesaplatır.

```text
POS → Kafka → Spark Structured Streaming → Cassandra

Tarayıcı → Spring Boot → Spark → Cassandra
                       ↳ MySQL (personel, yönetici ve departman)
                       ↳ HDFS (personel fotoğrafı)
```

## 1. Ubuntu’da başlat

Mevcut ortam Ubuntu 24.04 (WSL 2), OpenJDK 17, Gradle Wrapper, MySQL 8 ve Hadoop 3.3.6 kullanır. Betikler tek broker Kafka 3.9.1’i, bir master ve bir worker ile Spark 3.5.8’i ve tek düğümlü Cassandra 5.0.9’u Ubuntu’da doğrudan çalıştırır.

Komutları IntelliJ IDEA’nın Ubuntu terminalinde veya ayrı bir Ubuntu terminalinde çalıştır.

İlk kurulumda Apache paketlerini indirmek için:

```bash
cd ~/projeler/bigdata-pro3
bash scripts/install.sh
```

Sonraki açılışlarda proje klasörüne girip servisleri ve uygulamayı başlat:

```bash
cd ~/projeler/bigdata-pro3
bash scripts/start-services.sh
bash scripts/start-app.sh
```

`start-services.sh`, MySQL, HDFS, Kafka, Cassandra ve Spark master/worker süreçlerini hazırlar. Gerektiğinde Kafka topic’lerini, Cassandra tablosunu, MySQL başlangıç verilerini ve HDFS yer tutucu görselini oluşturur. `start-app.sh` projeyi Gradle ile derler, Spark harcama işini ve Spring Boot’u başlatır.

Terminalde `Spring Boot hazır.` mesajını gördükten sonra [uygulamanın ana sayfasını](http://localhost:8083/) aç. Soldaki listeden bir çalışanı seçebilir veya [SCOTT profiline](http://localhost:8083/employees/7788) doğrudan gidebilirsin.

| Bileşen | Adres veya konum |
|---|---|
| Spring Boot web uygulaması | [localhost:8083](http://localhost:8083/) |
| Spark master paneli | [localhost:8080](http://localhost:8080/) |
| Kafka bootstrap adresi | `127.0.0.1:9092` |
| Projenin MySQL örneği | `127.0.0.1:3307`, şema `sau_pro3` |
| Cassandra | `127.0.0.1:9042`, tablo `sau_pro3.expenses` |
| HDFS | `hdfs://localhost:9000`, fotoğraflar `/sau-pro3/photos` |
| Kafka/Cassandra verileri ve Spark checkpoint’i | `~/.local/share/sau-pro3/state` |
| Proje servislerinin günlükleri | `~/.local/share/sau-pro3/logs` |

## 2. POS kaydı gönder

POS kaydının alan sırası `user_id,date_time,description,type,count,payment` şeklindedir. `payment`, işlemde ödenen toplam tutardır. Yayıncı çalışanın departmanını MySQL’den bulur ve kaydı `accounting`, `research`, `sales` veya `operations` topic’ine gönderir. Verilen personel listesinde Operations çalışanı bulunmasa da `operations` topic’i oluşturulur.

### Belirli bir çalışana tek kayıt gönder

Bu örnek SCOTT’a (`7788`) 12,50 tutarında bir harcama gönderir:

```bash
printf '%s\n' "7788,$(date -Iseconds),Classroom demo,Food,1,12.50" \
  | bash scripts/pos.sh --stdin
```

Çıktıda `research 7788,` ile başlayan satırı gör. Birkaç saniye sonra [SCOTT profilini](http://localhost:8083/employees/7788) yenile. Bu sırada başka kayıt eklenmediyse işlem sayısı **1**, toplam **12,50** artar.

### Canlı POS demosu çalıştır

Her işlemde rastgele çalışan ve harcama seçmek için:

```bash
bash scripts/pos.sh --demo
```

Yalnızca SCOTT için rastgele harcama üretmek için:

```bash
bash scripts/pos.sh --demo 7788
```

İki moddan birini çalıştır. Seçili çalışan modunda MySQL’de kayıtlı başka bir numara da kullanabilirsin. Örneğin `--demo 7499` yalnızca ALLEN’a gönderir. POS, terminalde yaklaşık saniyede bir gönderim satırı gösterir. Web sayfasından sonucu görmek için ilgili personelin profilini yenile. Demoyu çalıştığı terminalde **Ctrl+C** ile durdur.

Spark geçerli kayıtları Cassandra’ya yazarken Kafka topic, partition ve offset değerlerini anahtara ekler. Aynı Kafka kaydı tekrar işlendiğinde aynı satır güncellenir. Spark toplamı bütün harcamalar üzerinden hesaplar ve web ekranı işlem listesini 25 satırlık sayfalar hâlinde gösterir.

Profili için JPEG yüklenmemiş çalışanlarda HDFS’deki `placeholder.png` kullanılır. Personel sayfasındaki **Upload photo** bölümüyle JPEG yüklenebilir.

## 3. Entegrasyon testini çalıştır

Canlı POS demosu kapalıyken aşağıdaki komutu çalıştır:

```bash
python3 -u scripts/verify.py
```

`PASS` satırları dört Kafka topic’ini, MySQL’e göre departman yönlendirmesini, hatalı ödeme satırının elenmesini, Cassandra’ya yazmayı, Spark toplamını, personel sayfasını ve HDFS fotoğraf işlemini doğrular.

Test kayıtları Cassandra’da kalır. Her başarılı çalıştırma SCOTT’a **2 işlem ve 49,75** ekler. Sonucu web sayfasında görmek için profili yenile.

## 4. IntelliJ IDEA’da aç

IntelliJ’de **File → Open** ile proje klasörünü aç:

```text
\\wsl.localhost\Ubuntu\home\elif\projeler\bigdata-pro3
```

Gradle ayarlarında **Distribution → Wrapper** ve **Gradle JVM → Ubuntu JDK 17** seç. Gerekirse JDK olarak aşağıdaki klasörü göster:

```text
\\wsl.localhost\Ubuntu\usr\lib\jvm\java-17-openjdk-amd64
```

Spring Boot’u IntelliJ’den çalıştırmak için altyapı servislerinin ve Spark işinin açık olması gerekir. Betikle başlatılan web uygulamasını durdurmak için `bash scripts/stop-app.sh --web-only` çalıştır. Ardından `edu.sau.pro3.Application` sınıfını Ubuntu hedefiyle ve `-Djava.net.preferIPv4Stack=true` VM seçeneğiyle çalıştır. Spark işini web uygulaması olmadan başlatmak için `bash scripts/start-app.sh --spark-only` kullan. [JetBrains IntelliJ IDEA WSL kılavuzu](https://www.jetbrains.com/help/idea/how-to-use-wsl-development-environment-in-product.html)

POS’u IntelliJ’den çalıştırmak için `edu.sau.pro3.PosPublisher` çalıştırma ayarlarında **Program arguments** alanına `--demo` veya `--demo 7788` yaz.

## 5. Servisleri kısa komutlarla kontrol et

Komutların kısa adlarını kullanmak için her yeni Ubuntu terminal sekmesinde bir kez aşağıdaki satırı çalıştır:

```bash
export PATH="$HOME/hadoop/bin:$HOME/.local/share/sau-pro3/dist/kafka_2.13-3.9.1/bin:$HOME/.local/share/sau-pro3/dist/apache-cassandra-5.0.9/bin:$PATH"
```

### HDFS, Cassandra ve Kafka

```bash
hdfs dfs -ls /sau-pro3/photos
hdfs dfsadmin -report -live
nodetool status
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

- HDFS fotoğrafları listeler ve raporda `Live datanodes (1)` görünür. Fotoğrafları [HDFS web panelinden](http://localhost:9870/explorer.html#/sau-pro3/photos) de listeleyebilirsin.
- Cassandra çıktısındaki tek `UN` satırı, düğümün ayakta ve normal durumda olduğunu gösterir.
- Kafka çıktısında `accounting`, `operations`, `research` ve `sales` görünür.

### MySQL personel bilgilerini doğrula

```bash
mysql -h 127.0.0.1 -P 3307 -u pro3 -p sau_pro3 -e \
"SELECT e.empno, e.ename, m.ename AS manager, d.dname AS department, e.sal, e.comm
 FROM emp e
 JOIN dept d ON d.deptno = e.deptno
 LEFT JOIN emp m ON m.empno = e.mgr
 WHERE e.empno = 7788;"
```

`Enter password:` sorulduğunda `pro3-local` yaz. Bu, kurulum betiğinin 3307 portundaki yerel MySQL örneğinde oluşturduğu `pro3` kullanıcısının demo parolasıdır. Yazdığın karakterler görünmez. Çıktıda **SCOTT**, **JONES**, **RESEARCH**, **3000.00** ve komisyon için `NULL` beklenir.

### Web uygulamasından HDFS fotoğrafını doğrula

```bash
hdfs dfs -ls /sau-pro3/photos/scott.jpg
curl -fsS -o /dev/null -w 'HTTP %{http_code} | %{content_type}\n' \
  http://127.0.0.1:8083/employees/7788/photo
```

SCOTT profiline JPEG yükledikten sonra HDFS’de `scott.jpg`, web yanıtında `HTTP 200 | image/jpeg` beklenir. Fotoğraf yüklenmemişse web uç noktası yer tutucuyu `image/png` olarak döndürür.

### Spark ve web uygulaması

```bash
curl -fsS http://127.0.0.1:8080/json/ | python3 -c \
  'import json,sys; d=json.load(sys.stdin); print("Live workers:", d["aliveworkers"]); print("Active apps:", [a["name"] for a in d["activeapps"]])'

curl -fsS http://127.0.0.1:8093/health
echo

curl -fsS -o /dev/null -w 'Web HTTP %{http_code}\n' \
  http://127.0.0.1:8083/
```

Beklenen sonuçlar `Live workers: 1`, aktif uygulamalarda `SAU Project 3 Expenses`, `{"streaming":true}` ve `Web HTTP 200`.

### Kafka mesajını ve Cassandra kaydını göster

Bir terminalde `research` topic’inden yeni mesajları izle:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic research
```

`--topic research` tüketici komutunun bir seçeneğidir. Yukarıdaki satırı tümüyle çalıştır. Terminal yeni mesaj gelene kadar boş bekler. İkinci Ubuntu sekmesinde proje klasöründen `bash scripts/pos.sh --demo 7788` çalıştırınca SCOTT’un kayıtları ilk sekmede görünür. Dinlemeyi durdurmak için **Ctrl+C**’ye bas.

Cassandra tarafında SCOTT’un kayıtlarını görmek için:

```bash
cqlsh 127.0.0.1 9042 -e \
  "SELECT empno, topic, occurred_at, description, payment
   FROM sau_pro3.expenses WHERE empno = 7788;"
```

Web uygulamasının Spark’tan aldığı raporu terminalde görmek için:

```bash
curl -fsS http://127.0.0.1:8083/api/employees/7788/expenses \
  | python3 -m json.tool
```

`total` ve `count` alanlarını SCOTT’un web profilindeki **Total expenses** ve **Transactions** değerleriyle karşılaştır.

## 6. Ek bilgi — Servisleri elle başlatma

Bu bölüm, ilk kurulumu tamamlanmış ve proje verileri hazırlanmış Ubuntu ortamı içindir. Servisler kapalıysa `start-services.sh` ile aynı programları tek tek başlatmak için bu komutları kullanabilirsin.

Önce Java ve servis dizinlerini aynı terminalde yükle:

```bash
cd ~/projeler/bigdata-pro3
source scripts/env.sh
```

`source` bu aşamada servis başlatmaz. `launch` fonksiyonu verilen komutu arka planda çalıştırır, süreç kimliğini kaydeder ve çıktıyı günlük dosyasına yazar.

### MySQL

```bash
launch mysql mysqld --no-defaults \
  --datadir="$pro3_home/state/mysql" --port=3307 \
  --socket="$run/mysql.sock" --pid-file="$run/mysqld.pid" \
  --bind-address=127.0.0.1 --mysqlx=OFF \
  --innodb-buffer-pool-size=64M --skip-log-bin
```

### HDFS

```bash
export HADOOP_HEAPSIZE_MAX=512
launch hdfs-namenode "$hadoop/bin/hdfs" namenode
launch hdfs-datanode "$hadoop/bin/hdfs" datanode
```

### Kafka

```bash
launch kafka env KAFKA_HEAP_OPTS="-Xms256m -Xmx512m" LOG_DIR="$logs" \
  "$kafka/bin/kafka-server-start.sh" "$run/kafka.properties"
```

`$run/kafka.properties`, proje betiğinin hazırladığı KRaft yapılandırmasıdır. Kafka açıldıktan sonra dört topic’i tek komut bloğuyla oluşturmak için:

```bash
for topic in accounting research sales operations; do
  "$kafka/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" \
    --partitions 1 --replication-factor 1
done
```

### Cassandra

```bash
launch cassandra env MAX_HEAP_SIZE=768M HEAP_NEWSIZE=192M CASSANDRA_LOG_DIR="$logs" \
  "$cassandra/bin/cassandra" -f "-Dcassandra.storagedir=$pro3_home/state/cassandra"
```

Cassandra hazır olduktan sonra tabloyu oluşturmak gerekirse:

```bash
"$cassandra/bin/cqlsh" 127.0.0.1 9042 -f data/schema.cql
```

### Spark master ve worker

```bash
export SPARK_PID_DIR="$run" SPARK_LOG_DIR="$logs" SPARK_IDENT_STRING=sau-pro3
export SPARK_DAEMON_MEMORY=384m

"$spark/sbin/start-master.sh" --host 127.0.0.1 --port 7077 --webui-port 8080

"$spark/sbin/start-worker.sh" --host 127.0.0.1 --cores 2 --memory 1536m \
  --work-dir "$pro3_home/state/spark-worker" --webui-port 8081 \
  spark://127.0.0.1:7077
```

Master ve worker altyapıyı çalıştırır. Kafka’dan harcama okuyan Java işi ile web uygulamasını başlatmak için servisler hazır olduktan sonra `bash scripts/start-app.sh` çalıştır.

### Hocanın yardım dosyasındaki komutların bu projedeki karşılığı

| Yardım dosyasındaki örnek | Bu projedeki karşılığı |
|---|---|
| `systemctl` ile Kafka ve ZooKeeper | Kafka, `kafka-server-start.sh` ve projenin KRaft yapılandırmasıyla başlar. |
| Büyük harfli `SALES`, `RESEARCH` gibi topic adları | Projenin yayıncısı ve tüketicisi `sales`, `research`, `accounting`, `operations` adlarını kullanır. Kafka’da büyük ve küçük harfli adlar ayrı topic’lerdir. |
| `kafka-console-consumer.sh` ile topic’i izleme | Aynı araç kullanılır. Örneğin `--topic research` ile Research kayıtları izlenir. |
| Hadoop ve YARN başlatma | HDFS için NameNode ve DataNode açılır. Spark kendi Standalone master/worker süreçlerini kullanır. |
| `spark-shell` içinde Kafka `readStream` örneği | Java’daki `SparkExpenses`, `spark-submit` ile başlar ve dört topic’i birlikte dinler. |
| `systemctl start cassandra` ve `spark-shell` ile `test.emp` tablosu üzerinde deneme | Cassandra proje dizinindeki dağıtımdan başlar. `SparkExpenses`, Cassandra’ya `sau_pro3.expenses` tablosu üzerinden bağlanır. |
| Spark paketlerini `--packages` ile yükleme | `start-app.sh`, Spark’a Kafka bağlayıcısı **3.5.8** ve Cassandra bağlayıcısı **3.5.1** verir. |

Yardım dosyasındaki `spark-shell` örnekleri bağlantıları adım adım göstermek içindir. Bu projede `spark-submit`, Kafka okuma ve Cassandra yazma işlemlerini uygulama başlarken yükler. Ubuntu’nun Python sürümünü değiştirmeye gerek yoktur.

## 7. Gösterimden sonra durdur

- Sahte POS veya Kafka konsol tüketicisini çalıştığı sekmede **Ctrl+C** ile durdur.
- Spring Boot ve Spark harcama işini birlikte durdurmak için:

```bash
bash scripts/stop-app.sh
```

Sadece Spring Boot’u durdurmak için `bash scripts/stop-app.sh --web-only` kullan. Bu komutlardan sonra Kafka, Cassandra, MySQL, HDFS ve Spark master/worker açık kalır.

---

# SWE 307 — Big Data Project 3 (English)

This project collects employee expenses from a simulated point-of-sale (POS) system. The publisher looks up the employee’s department in MySQL and sends the expense to the corresponding Kafka topic. Spark Structured Streaming reads the event and stores it in Cassandra. When an employee profile opens, Spring Boot fetches personnel details from MySQL, retrieves the photo from HDFS, and requests the expense total from Spark.

```text
POS → Kafka → Spark Structured Streaming → Cassandra

Browser → Spring Boot → Spark → Cassandra
                     ↳ MySQL (employee, manager, and department)
                     ↳ HDFS (employee photo)
```

## 1. Start the project on Ubuntu

The project uses Ubuntu 24.04 (WSL 2), OpenJDK 17, Gradle Wrapper, MySQL 8, Hadoop 3.3.6, Kafka 3.9.1, Spark 3.5.8, and Cassandra 5.0.9. Spark runs with one master and one worker. HDFS has one DataNode, Cassandra has one node, and Kafka has one broker.

Run the commands in IntelliJ IDEA’s Ubuntu terminal or a separate Ubuntu terminal.

For the initial package installation:

```bash
cd ~/projeler/bigdata-pro3
bash scripts/install.sh
```

To start the installed services and application:

```bash
cd ~/projeler/bigdata-pro3
bash scripts/start-services.sh
bash scripts/start-app.sh
```

`start-services.sh` starts MySQL, HDFS, Kafka, Cassandra, and the Spark master and worker. It also prepares the Kafka topics, the Cassandra table, the initial MySQL data, and the HDFS placeholder image as needed.

`start-app.sh` builds the project with Gradle and starts the Spark expense job and Spring Boot. When the terminal prints `Spring Boot hazır.` (Spring Boot is ready), open the [application home page](http://localhost:8083/) and select an employee.

| Component | Address or location |
|---|---|
| Web application | [localhost:8083](http://localhost:8083/) |
| Example employee profile | [SCOTT (7788)](http://localhost:8083/employees/7788) |
| Spark master UI | [localhost:8080](http://localhost:8080/) |
| Kafka bootstrap server | `127.0.0.1:9092` |
| MySQL | `127.0.0.1:3307`, schema `sau_pro3` |
| Cassandra | `127.0.0.1:9042`, table `sau_pro3.expenses` |
| HDFS | `hdfs://localhost:9000`, photo directory `/sau-pro3/photos` |
| Kafka and Cassandra data and Spark checkpoint | `~/.local/share/sau-pro3/state` |
| Service logs | `~/.local/share/sau-pro3/logs` |

## 2. Send POS expenses

Each POS record contains `user_id,date_time,description,type,count,payment`. The `payment` field is the amount paid for the entire transaction. The publisher routes each expense to `accounting`, `research`, `sales`, or `operations` based on the employee’s department. The supplied personnel list contains no Operations employees, so that topic can remain empty during the demo.

### Send one expense to a specific employee

This command sends a single expense of **12.50** for SCOTT (`7788`):

```bash
printf '%s\n' "7788,$(date -Iseconds),Classroom demo,Food,1,12.50" \
  | bash scripts/pos.sh --stdin
```

The publisher prints a line beginning with `research 7788,`. After a few seconds, refresh [SCOTT’s profile](http://localhost:8083/employees/7788). If no other transactions came in, **Transactions** increases by **1** and **Total expenses** increases by **12.50**.

### Run the live POS demo

Generate random expenses for randomly selected employees:

```bash
bash scripts/pos.sh --demo
```

Generate random expenses only for SCOTT:

```bash
bash scripts/pos.sh --demo 7788
```

Run either mode. The selected-employee mode accepts any employee number registered in MySQL. For example, `--demo 7499` generates expenses only for ALLEN. The terminal displays a new record approximately once a second. Refresh the employee profile to see the updated list and total. Press **Ctrl+C** in the POS terminal to stop the demo.

Spark stores each record using the Kafka topic, partition, and offset as part of its Cassandra key. If Spark reprocesses the same Kafka record, it writes to the same row. The expense total covers all records, and the web page displays 25 transactions per page.

To add a photo, open an employee profile and use **Upload photo** to save a JPEG to HDFS. Profiles without an uploaded photo use `placeholder.png`.

## 3. Run the integration test

Stop the live POS demo before running:

```bash
python3 -u scripts/verify.py
```

The `PASS` lines confirm the Kafka topics, department routing, invalid payment filtering, Cassandra writes, Spark totals, rendered employee profile, and HDFS photo handling.

Test records remain in Cassandra. Each successful run adds **2 transactions and 49.75** to SCOTT’s total. Refresh the profile to see the result.

## 4. Open the project in IntelliJ IDEA

Select **File → Open** and open this folder:

```text
\\wsl.localhost\Ubuntu\home\elif\projeler\bigdata-pro3
```

In the Gradle settings, select **Wrapper** as the distribution and **Ubuntu JDK 17** as the Gradle JVM. If IntelliJ asks for the JDK location, use:

```text
\\wsl.localhost\Ubuntu\usr\lib\jvm\java-17-openjdk-amd64
```

To run Spring Boot directly from IntelliJ, keep the infrastructure services and Spark job running. Stop the web process started by the script with `bash scripts/stop-app.sh --web-only`. Then run `edu.sau.pro3.Application` in Ubuntu with the VM option `-Djava.net.preferIPv4Stack=true`. To start only the Spark job, use `bash scripts/start-app.sh --spark-only`. [JetBrains IntelliJ IDEA WSL guide](https://www.jetbrains.com/help/idea/how-to-use-wsl-development-environment-in-product.html)

To run the POS publisher directly from IntelliJ, open the run configuration for `edu.sau.pro3.PosPublisher` and set **Program arguments** to `--demo` or `--demo 7788`.

## 5. Check the services with short commands

Add the tools to `PATH` once in each new Ubuntu terminal tab:

```bash
export PATH="$HOME/hadoop/bin:$HOME/.local/share/sau-pro3/dist/kafka_2.13-3.9.1/bin:$HOME/.local/share/sau-pro3/dist/apache-cassandra-5.0.9/bin:$PATH"
```

### HDFS, Cassandra, and Kafka

```bash
hdfs dfs -ls /sau-pro3/photos
hdfs dfsadmin -report -live
nodetool status
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

- HDFS lists the photo files and reports `Live datanodes (1)`. The directory is also available in the [HDFS web interface](http://localhost:9870/explorer.html#/sau-pro3/photos).
- Cassandra reports one `UN` node. `UN` means the node is up and in the normal state.
- Kafka lists `accounting`, `operations`, `research`, and `sales`.

### Verify employee details in MySQL

```bash
mysql -h 127.0.0.1 -P 3307 -u pro3 -p sau_pro3 -e \
"SELECT e.empno, e.ename, m.ename AS manager, d.dname AS department, e.sal, e.comm
 FROM emp e
 JOIN dept d ON d.deptno = e.deptno
 LEFT JOIN emp m ON m.empno = e.mgr
 WHERE e.empno = 7788;"
```

At the `Enter password:` prompt, enter `pro3-local`. The setup script assigns this demo password to the `pro3` user on the local MySQL instance at port 3307. Characters are hidden as you type. Expect **SCOTT**, **JONES**, **RESEARCH**, **3000.00**, and `NULL` for commission.

### Verify the HDFS photo through the web application

```bash
hdfs dfs -ls /sau-pro3/photos/scott.jpg
curl -fsS -o /dev/null -w 'HTTP %{http_code} | %{content_type}\n' \
  http://127.0.0.1:8083/employees/7788/photo
```

After uploading a JPEG on SCOTT’s profile, expect `scott.jpg` in HDFS and `HTTP 200 | image/jpeg` from the web endpoint. For a profile without an uploaded photo, the endpoint serves the placeholder as `image/png`.

### Spark and Spring Boot

```bash
curl -fsS http://127.0.0.1:8080/json/ | python3 -c \
  'import json,sys; d=json.load(sys.stdin); print("Live workers:", d["aliveworkers"]); print("Active apps:", [a["name"] for a in d["activeapps"]])'

curl -fsS http://127.0.0.1:8093/health
echo

curl -fsS -o /dev/null -w 'Web HTTP %{http_code}\n' \
  http://127.0.0.1:8083/
```

Expect `Live workers: 1`, `SAU Project 3 Expenses` in the active app list, `{"streaming":true}`, and `Web HTTP 200`.

### Observe a Kafka message and check the Cassandra record

Open a console consumer in one terminal:

```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic research
```

`--topic research` is an option for the consumer. Run the full line above. The terminal waits quietly for new messages. In a second Ubuntu tab, run `bash scripts/pos.sh --demo 7788` from the project directory to see SCOTT’s records in the first tab. Press **Ctrl+C** to stop the consumer.

To check SCOTT’s records in Cassandra:

```bash
cqlsh 127.0.0.1 9042 -e \
  "SELECT empno, topic, occurred_at, description, payment
   FROM sau_pro3.expenses WHERE empno = 7788;"
```

To inspect the expense report that the web application gets from Spark:

```bash
curl -fsS http://127.0.0.1:8083/api/employees/7788/expenses \
  | python3 -m json.tool
```

Compare `total` and `count` with **Total expenses** and **Transactions** on SCOTT’s profile.

## 6. Optional reference — Start the services individually

Use these commands when the initial setup is complete, project data has been initialized, and the services are stopped. Run them in the same Ubuntu terminal so the environment variables stay available.

```bash
cd ~/projeler/bigdata-pro3
source scripts/env.sh
```

`source` loads the project paths, Java settings, and the `launch` helper. The helper starts a process in the background, saves its process ID, and logs its output.

### MySQL

```bash
launch mysql mysqld --no-defaults \
  --datadir="$pro3_home/state/mysql" --port=3307 \
  --socket="$run/mysql.sock" --pid-file="$run/mysqld.pid" \
  --bind-address=127.0.0.1 --mysqlx=OFF \
  --innodb-buffer-pool-size=64M --skip-log-bin
```

### HDFS

```bash
export HADOOP_HEAPSIZE_MAX=512
launch hdfs-namenode "$hadoop/bin/hdfs" namenode
launch hdfs-datanode "$hadoop/bin/hdfs" datanode
```

### Kafka

```bash
launch kafka env KAFKA_HEAP_OPTS="-Xms256m -Xmx512m" LOG_DIR="$logs" \
  "$kafka/bin/kafka-server-start.sh" "$run/kafka.properties"
```

After Kafka is ready, create the four topics if needed:

```bash
for topic in accounting research sales operations; do
  "$kafka/bin/kafka-topics.sh" --bootstrap-server localhost:9092 \
    --create --if-not-exists --topic "$topic" \
    --partitions 1 --replication-factor 1
done
```

### Cassandra

```bash
launch cassandra env MAX_HEAP_SIZE=768M HEAP_NEWSIZE=192M CASSANDRA_LOG_DIR="$logs" \
  "$cassandra/bin/cassandra" -f "-Dcassandra.storagedir=$pro3_home/state/cassandra"
```

After Cassandra is ready, run the schema script if the table needs to be created:

```bash
"$cassandra/bin/cqlsh" 127.0.0.1 9042 -f data/schema.cql
```

### Spark master and worker

```bash
export SPARK_PID_DIR="$run" SPARK_LOG_DIR="$logs" SPARK_IDENT_STRING=sau-pro3
export SPARK_DAEMON_MEMORY=384m

"$spark/sbin/start-master.sh" --host 127.0.0.1 --port 7077 --webui-port 8080

"$spark/sbin/start-worker.sh" --host 127.0.0.1 --cores 2 --memory 1536m \
  --work-dir "$pro3_home/state/spark-worker" --webui-port 8081 \
  spark://127.0.0.1:7077
```

When the services are ready, run `bash scripts/start-app.sh` to start the Spark expense job and the web application.

### How the course examples map to this project

| Course example | Project implementation |
|---|---|
| Start Kafka and ZooKeeper with `systemctl` | Start Kafka with `kafka-server-start.sh` and the project’s KRaft configuration. |
| Create uppercase topics such as `SALES` and `RESEARCH` | The project uses lowercase `accounting`, `research`, `sales`, and `operations`. Kafka topic names are case-sensitive. |
| View messages with `kafka-console-consumer.sh` | Use the same tool with a project topic such as `--topic research`. |
| Start Hadoop DFS and YARN | Start the HDFS NameNode and DataNode. Spark uses its own Standalone master and worker. |
| Read Kafka with `readStream` in `spark-shell` | The Java `SparkExpenses` job runs with `spark-submit` and subscribes to all four topics. |
| Start Cassandra with `systemctl` and test the connection using `test.emp` | Start Cassandra from the installed distribution. The Spark job reads and writes `sau_pro3.expenses`. |
| Load Spark connectors with `--packages` | `start-app.sh` supplies the Kafka connector **3.5.8** and Cassandra connector **3.5.1**. |

The course examples demonstrate Kafka consumption and Cassandra access interactively. The project performs these operations in the Java Spark job. The existing Ubuntu Python version is sufficient for the helper scripts.

## 7. Stop the demo or application

Press **Ctrl+C** in the terminal running the POS demo or Kafka console consumer to stop that process.

To stop Spring Boot and the Spark expense job:

```bash
bash scripts/stop-app.sh
```

To stop only Spring Boot, use `bash scripts/stop-app.sh --web-only`. Kafka, Cassandra, MySQL, HDFS, and the Spark master and worker continue running after either command.