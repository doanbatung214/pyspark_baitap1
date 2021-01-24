# pyspark_baitap1

# Spark và Mapreduce

## 1. Apache Spark
### 1.1 Tìm hiểu về Apache Spark
<p align="left"> Apache Spark là một hệ thống mã nguồn mở cho phép thực hiện tính toán trên cụm nhằm tạo ra khả năng phân tích dữ liệu nhanh với 2 tiêu chí: nhanh về cả lúc chạy và nhanh cả lúc ghi dữ liệu. </p>

<p align="left"> Nó được phát triển sơ khởi vào năm 2009 bởi AMPLab tại đại học California. Sau này, Spark đã được trao cho Apache Software Foundation vào năm 2013 và được phát triển cho đến nay. Nó cho phép xây dựng các mô hình dự đoán nhanh chóng với việc tính toán được thực hiện trên một nhóm các máy tính, có có thể tính toán cùng lúc trên toàn bộ tập dữ liệu mà không cần phải trích xuất mẫu tính toán thử nghiệm. Tốc độ xử lý của Spark có được do việc tính toán được thực hiện cùng lúc trên nhiều máy khác nhau. Đồng thời việc tính toán được thực hiện ở bộ nhớ trong (in-memories) hay thực hiện hoàn toàn trên RAM. </p>

<p align="center"> <img src ="https://databricks.com/wp-content/uploads/2019/02/largest-open-source-apache-spark.png" />
  
<p align="center"> Tổng quan về Spark </p>

<p align="left"> Để chạy chương trình nhanh hơn, Spark cung cấp một mô hình thực thi cho phép tối ưu các tính toán đồ thị một cách tùy ý (optimize arbitrary operator graphs) và hỗ trợ tính toán tại bộ nhớ trong giúp việc query dữ liệu nhanh hơn công nghệ tính toán dựa trên bộ nhớ ngoài (disk-based) như là Hadoop.
Để việc lập trình được đơn giản, Spark cung cấp một bộ API cho các ngôn ngữ Scala, Java và Python. Ngoài ra, chúng ta có thể tương tác với Spark bằng việc sử dụng Scala và Python shell để query dữ liệu tiện lợi hơn. </p>

<p align="left"> Spark là một công nghệ mới nhưng có thể truy cập bất cứ nguồn dữ liệu nào hỗ trợ Hadoop. Điều này giúp chúng ta có thể dễ dạng chạy Spark trên nguồn dữ liệu sẵn hiện có. </p>

### 1.2 Thành phần của Spark

<p> Apache Spark gồm có 5 thành phần chính : Spark Core, Spark Streaming, Spark SQL, MLlib và GraphX, trong đó: 

+ Spark Core là nền tảng cho các thành phần còn lại và các thành phần này muốn khởi chạy được thì đều phải thông qua Spark Core do Spark Core đảm nhận vai trò thực hiện công việc tính toán và xử lý trong bộ nhớ (In-memory computing) đồng thời nó cũng tham chiếu các dữ liệu được lưu trữ tại các hệ thống lưu trữ bên ngoài. 

+ Spark SQL cung cấp một kiểu data abstraction mới (SchemaRDD) nhằm hỗ trợ cho cả kiểu dữ liệu có cấu trúc (structured data) và dữ liệu nửa cấu trúc (semi-structured data – thường là dữ liệu dữ liệu có cấu trúc nhưng không đồng nhất và cấu trúc của dữ liệu phụ thuộc vào chính nội dung của dữ liệu ấy). Spark SQL hỗ trợ DSL (Domain-specific language) để thực hiện các thao tác trên DataFrames bằng ngôn ngữ Scala, Java hoặc Python và nó cũng hỗ trợ cả ngôn ngữ SQL với giao diện command-line và ODBC/JDBC server.

+ Spark Streaming được sử dụng để thực hiện việc phân tích stream bằng việc coi stream là các mini-batches và thực hiệc kỹ thuật RDD transformation đối với các dữ liệu mini-batches này. Qua đó cho phép các đoạn code được viết cho xử lý batch có thể được tận dụng lại vào trong việc xử lý stream, làm cho việc phát triển lambda architecture được dễ dàng hơn. Tuy nhiên điều này lại tạo ra độ trễ trong xử lý dữ liệu (độ trễ chính bằng mini-batch duration) và do đó nhiều chuyên gia cho rằng Spark Streaming không thực sự là công cụ xử lý streaming giống như Storm hoặc Flink.

+ MLlib (Machine Learning Library): MLlib là một nền tảng học máy phân tán bên trên Spark do kiến trúc phân tán dựa trên bộ nhớ. Theo các so sánh benchmark Spark MLlib nhanh hơn 9 lần so với phiên bản chạy trên Hadoop (Apache Mahout).

+ GrapX: Grapx là nền tảng xử lý đồ thị dựa trên Spark. Nó cung cấp các Api để diễn tảcác tính toán trong đồ thị bằng cách sử dụng Pregel Api.
</p>

<p align="center"> <img src ="https://images.viblo.asia/full/2ba27584-446d-49d8-b9bb-fe876ecb60d5.png" />
<p align="center"> Các thành phần của Spark </p>

### 1.3 Những đặc điểm nổi bật của Spark

<p>
+ Xử lý dữ liệu: Spark xử lý dữ liệu theo lô và thời gian thực
 
+ Tính tương thích: Có thể tích hợp với tất cả các nguồn dữ liệu và định dạng tệp được hỗ trợ bởi cụm Hadoop.

+ Hỗ trợ ngôn ngữ: hỗ trợ Java, Scala, Python và R.

+ Phân tích thời gian thực:

+ Apache Spark có thể xử lý dữ liệu thời gian thực tức là dữ liệu đến từ các luồng sự kiện thời gian thực với tốc độ hàng triệu sự kiện mỗi giây. Ví dụ: Data Twitter chẳng hạn hoặc luợt chia sẻ, đăng bài trên Facebook. Sức mạnh Spark là khả năng xử lý luồng trực tiếp hiệu quả.

+ Apache Spark có thể được sử dụng để xử lý phát hiện gian lận trong khi thực hiện các giao dịch ngân hàng. Đó là bởi vì, tất cả các khoản thanh toán trực tuyến được thực hiện trong thời gian thực và chúng ta cần ngừng giao dịch gian lận trong khi quá trình thanh toán đang diễn ra.

+ Mục tiêu sử dụng:
    
    1.Xử lý dữ liệu nhanh và tương tác
  
    2.Xử lý đồ thị
  
    3.Công việc lặp đi lặp lại

    4.Xử lý thời gian thực

    5.joining Dataset

    6.Machine Learning

    7.Apache Spark là Framework thực thi dữ liệu dựa trên Hadoop HDFS. Apache Spark không thay thế cho Hadoop nhưng nó là một framework ứng dụng. Apache Spark tuy ra đời sau nhưng được nhiều người biết đến hơn Apache Hadoop vì khả năng xử lý hàng loạt và thời gian thực.
    
## 2. Mapreduce
### 2.1 Khái niệm
<p>
MapReduce là mô hình được thiết kế độc quyền bởi Google, nó có khả năng lập trình xử lý các tập dữ liệu lớn song song và phân tán thuật toán trên 1 cụm máy tính. MapReduce trở thành một trong những thành ngữ tổng quát hóa trong thời gian gần đây.

MapReduce sẽ  bao gồm những thủ tục sau: thủ tục 1 Map() và 1 Reduce(). Thủ tục Map() bao gồm lọc (filter) và phân loại (sort) trên dữ liệu khi thủ tục khi thủ tục Reduce() thực hiện quá trình tổng hợp dữ liệu. Đây là mô hình dựa vào các khái niệm biển đối của bản đồ và reduce những chức năng lập trình theo hướng chức năng. Thư viện của thủ tục Map() và Reduce() sẽ được viết bằng nhiều loại ngôn ngữ khác nhau. Thủ tục được cài đặt miễn phí và được sử dụng phổ biến nhất là là Apache Hadoop.

</p>

### 2.2 Các hàm chính của Mapreduce
<p>
MapReduce có 2 hàm chính là Map() và Reduce(), đây là 2 hàm đã được định nghĩa bởi người dùng và nó cũng chính là 2 giai đoạn liên tiếp trong quá trình xử lý dữ liệu của MapReduce. Nhiệm vụ cụ thể của từng hàm như sau: 

+ Hàm Map(): có nhiệm vụ nhận Input cho các cặp giá trị/  khóa và output chính là tập những cặp giá trị/khóa trung gian. Sau đó, chỉ cần ghi xuống đĩa cứng và tiến hành thông báo cho các hàm Reduce() để trực tiếp nhận dữ liệu. 
+ Hàm Reduce(): có nhiệm vụ tiếp nhận từ khóa trung gian và những giá trị tương ứng với lượng từ khóa đó. Sau đó, tiến hành ghép chúng lại để có thể tạo thành một tập khóa khác nhau. Các cặp khóa/giá trị này thường sẽ thông qua một con trỏ vị trí để đưa vào các hàm reduce. Quá trình này sẽ giúp cho lập trình viên quản lý dễ dàng hơn một lượng danh sách cũng như  phân bổ giá trị sao cho  phù hợp nhất với bộ nhớ hệ thống. 
+ Ở giữa Map và Reduce thì còn 1 bước trung gian đó chính là Shuffle. Sau khi Map hoàn thành  xong công việc của mình thì Shuffle sẽ làm nhiệm vụ chính là thu thập cũng như tổng hợp từ khóa/giá trị trung gian đã được map sinh ra trước đó rồi chuyển qua cho Reduce tiếp tục xử lý.

<p align="center"> <img src ="https://blog.itnavi.com.vn/wp-content/uploads/2020/06/Mapreduce-l%C3%A0-g%C3%AC-2.jpg" />
<p align="center"> Các hàm của Mapreduce </p>

### 2.3 Các ưu điểm nổi bật của Mapreduce
<p>
+ MapReduce có khả năng xử lý dễ dàng mọi bài toán có lượng dữ liệu lớn nhờ khả năng tác vụ phân tích và tính toán phức tạp. Nó có thể xử lý nhanh chóng cho ra kết quả dễ dàng chỉ trong khoảng thời gian ngắn.
+ Mapreduce có khả năng chạy song song trên các máy có sự phân tán  khác nhau. Với khả năng hoạt động độc lập kết hợp  phân tán, xử lý các lỗi kỹ thuật để mang lại nhiều hiệu quả cho toàn hệ thống. 
+ MapRedue có khả năng thực hiện trên nhiều nguồn ngôn ngữ lập trình khác nhau như: Java, C/ C++, Python, Perl, Ruby,… tương ứng với nó là những thư viện hỗ trợ. 
+ Các ứng dụng MapReduce dần hướng đến quan tâm nhiều hơn cho việc phát hiện các mã độc để có thể xử lý chúng. Nhờ vậy, hệ thống mới có thể vận hành trơn tru và được bảo mật nhất.
  
<p align="center"> <img src ="https://blog.itnavi.com.vn/wp-content/uploads/2020/06/Mapreduce-l%C3%A0-g%C3%AC-3.jpg" />
<p align="center"> Mapreduce có khả năng xử lý nhanh chóng lượng dữ liệu lớn </p>

### 2.4 Nguyên tắc hoạt động

Mapreduce hoạt động dựa vào nguyên tắc chính là “Chia để trị”, như sau:

+ Phân chia các dữ liệu cần xử lý thành nhiều phần nhỏ trước khi thực hiện. 
+ Xử lý các vấn đề nhỏ theo phương thức song song trên các máy tính rồi phân tán hoạt động theo hướng độc lập.
+ Tiến hành tổng hợp những kết quả thu được để đề ra được kết quả sau cùng. 

### 2.5 Các bước hoạt động của MapReduce
+ Bước 1: Tiến hành chuẩn bị các dữ liệu đầu vào để cho Map() có thể xử lý.
+ Bước 2: Lập trình viên thực thi các mã Map() để xử  lý. 
+ Bước 3: Tiến hành trộn lẫn các dữ liệu được xuất ra bởi Map() vào trong Reduce Processor
+ Bước 4: Tiến hành thực thi tiếp mã Reduce() để có thể xử lý tiếp các dữ liệu cần thiết.  
+ Bước 5: Thực hiện tạo các dữ liệu xuất ra cuối cùng. 

### 2.6 Một số công việc có thể sử dụng MapReduce
+ Thực hiện thống kê cho các từ khóa được xuất hiện ở trong các tài liệu, bài viết, văn bản hoặc được cập nhật trên hệ thống fanpage, website,…
+ Khi số lượng các bài viết đã được thống kê thì tài liệu sẽ có chứa các từ khóa đó. 
+ Sẽ có thể thống kê được những câu lệnh match, pattern bên trong các tài liệu đó
+ Khi thống kê được số lượng các URLs có xuất hiện bên trong một webpages. 
+ Sẽ thống kê được các lượt truy cập của khách hàng sao cho nó có thể tương ứng với các URLs.
+ Sẽ thống kê được tất cả từ khóa có trên website, hostname,…
## 3. Tài liệu tham khảo
    1. https://dataartblog.wordpress.com/2014/11/20/gioi-thieu-ve-apache-spark/
    2. https://en.wikipedia.org/wiki/Apache_Spark
    3. https://en.wikipedia.org/wiki/MapReduce
## 4. Bài tập: Tạo Project trên Colab dùng Spark đọc vào một file văn bản và đếm số từ trên bản, lọc ra k từ có tần suất xuất hiện nhiều nhất.
<p> Bài tập được thực hiện trong file bt1_wordcount_week1.ipynb. </p>
