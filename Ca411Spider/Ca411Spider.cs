using System;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using System.IO;
using System.Net;
using System.Threading;
using System.Collections.Concurrent;

using NPOI.HSSF.UserModel;
using NPOI.XSSF.UserModel;
using NPOI.SS.UserModel;
using HtmlAgilityPack;


namespace ZxlSpider.Ca411Spider
{
    public class Ca411Spider
    {
        private ConfigParams _configParams = null;
        private const string LINE = "------------------------------------------------------------------------------";
        private string _excelPath = null; // excel文件路径
        private int _fromRowNumber = 1; // 从excel表格的第几行开始查询
        private IWorkbook _workbook = null;
        private System.Diagnostics.Stopwatch _mainWatch = new System.Diagnostics.Stopwatch(); // 程序执行总时间计时器
        private System.Diagnostics.Stopwatch _stepWatch = new System.Diagnostics.Stopwatch(); // 程序单步执行计时器

        private int _requestCount = 0; // 需要发送的请求总个数（excel中已查询过的不计在内）
        private int _responseCount = 0; // response计数器（多线程递增时需原子操作）

        //public static 的Queue线程安全
        //public static Queue<RequestInfo> _requestQueue = new Queue<RequestInfo>(); 
        //public static Queue<ResponseInfo> _responseQueue = new Queue<ResponseInfo>();
        public ConcurrentQueue<RequestInfo> _requestQueue = new ConcurrentQueue<RequestInfo>();
        public ConcurrentQueue<ResponseInfo> _responseQueue = new ConcurrentQueue<ResponseInfo>();

        public Ca411Spider(string filePath, int fromRowNumber)
        {
            this._excelPath = filePath;
            this._fromRowNumber = fromRowNumber < 1 ? 1 : fromRowNumber;
            int a = Convert.ToInt32(ConfigurationManager.AppSettings["batch_send_request_count"]);
            int b = Convert.ToInt32(ConfigurationManager.AppSettings["batch_send_request_interval"]);
            int c = Convert.ToInt32(ConfigurationManager.AppSettings["batch_save_result_count"]);
            this._configParams = new ConfigParams(a, b, c);
        }

        public void Search()
        {
            _mainWatch.Start();
            _stepWatch.Start();

            if (this._excelPath.EndsWith(".xls") == false)
            {
                throw new ArgumentException("文件名不合法：请确保输入文件是Excel 2003格式的文件（*.xls）");
            }

            bool hasException = false;
            ISheet sheet = null;
            try
            {
                _stepWatch.Restart();
                InitWorkbook();
                sheet = _workbook.GetSheetAt(0);
                if (this._fromRowNumber > sheet.LastRowNum) throw new ArgumentException("起始行号输入不合法，已超过excel表格的最大行数");
                _stepWatch.Stop();
                Console.WriteLine("Excel读取完毕，共有{1}条记录，耗时：{0}", _stepWatch.Elapsed, sheet.LastRowNum);

                _stepWatch.Restart();
                RequestEnqueue(sheet);
                _stepWatch.Stop();
                Console.WriteLine("请求队列构造完毕，共{1}个请求，耗时：{0}", _stepWatch.Elapsed, _requestQueue.Count);
                Console.WriteLine("=>每次并发{0}个请求", _configParams.BatchSendRequestCount);
                Console.WriteLine("=>每2次批量发送请求的时间间隔为{0}秒", _configParams.BatchSendRequestInterval / 1000);
                Console.WriteLine("=>每命中{0}条查询结果保存一次excel", _configParams.BatchSaveResultCount);


                // 写线程
                Thread writingThread = new Thread(() => ProcessWriting(sheet));
                writingThread.IsBackground = true;
                writingThread.Start();

                SendRequest();
            }
            catch (Exception ex)
            {
                hasException = true;
                Console.WriteLine("Exception: " + ex.Message);
            }
            finally
            {
                _mainWatch.Stop();
                string msg = hasException ? "程序主线程异常中断" : "所有请求已发送完毕，主线程执行结束";
                Console.WriteLine(LINE);
                Console.WriteLine("{0}，共耗时：{1}", msg, _mainWatch.Elapsed);
                Console.WriteLine(LINE);
            }
        }

        private Query411Result GetWebResponse(string url, string encodeType)
        {
            Query411Result result = new Query411Result();
            string responseHtml = "";
            try
            {
                WebRequest request = WebRequest.Create(url);
                WebResponse response = request.GetResponse();
                StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.GetEncoding(encodeType));

                responseHtml = reader.ReadToEnd();
                result.IsSuccess = true;

                reader.Close();
                reader.Dispose();
                response.Close();
            }
            catch (Exception ex)
            {
                result.IsSuccess = false;
                responseHtml = "查无此人";
            }
            result.Message = responseHtml;
            return result;
        }

        // 并发请求
        private void SendRequest()
        {
            while (_requestQueue.Count > 0)
            {
                Console.WriteLine(LINE);
                // 从线程池中取SEND_REQUEST_COUNT个线程，用来发送请求获取响应
                for (int i = 0; i < _configParams.BatchSendRequestCount; i++)
                {
                    if (_requestQueue.Count > 0)
                    {
                        RequestInfo request = null;
                        bool success = _requestQueue.TryDequeue(out request);
                        if (success)
                        {
                            System.Threading.ThreadPool.QueueUserWorkItem(ResponseEnqueue, request);
                            Console.WriteLine("行号[{0}]：请求已发送完毕", request.ExcelRowNumber);
                            System.Threading.Thread.Sleep(5);
                        }
                        else
                        {
                            Console.WriteLine("SendRequest()异常：_requestQueue.TryDequeue(out request)失败");
                        }
                    }
                }
                Console.WriteLine(LINE);
                System.Threading.Thread.Sleep(_configParams.BatchSendRequestInterval);
            }
        }

        // [0:firstName, 1:secondName, 2:telephone]
        private string[] GetResponseInfo(string url, int excelRowNumber)
        {
            string[] result = null;
            string name = null;
            string telephone = null;
            try
            {
                Query411Result query411Result = GetWebResponse(url, "utf-8");
                if (!query411Result.IsSuccess)
                {
                    Console.WriteLine("行号[{0}]：{1}", excelRowNumber, query411Result.Message);
                    return null;
                }

                HtmlDocument doc = new HtmlDocument();
                doc.LoadHtml(query411Result.Message);
                //HtmlNode node = doc.GetElementbyId("total_result_found");
                HtmlNode node = doc.DocumentNode.SelectSingleNode("//div[@class='founditem_content reverse']");
                if (node != null)
                {
                    HtmlNode childNode = HtmlNode.CreateNode(node.OuterHtml);
                    name = node.SelectSingleNode("//meta[@itemprop='name']").Attributes["content"].Value;
                    telephone = node.SelectSingleNode("//meta[@itemprop='telephone']").Attributes["content"].Value;

                    int idx = name.IndexOf(" ");
                    string firstName = idx < 0 ? "" : name.Substring(0, idx);
                    string lastName = idx < 0 ? name : name.Substring(idx + 1).TrimStart(' ');

                    result = new string[3] { firstName, lastName, telephone };
                }

                string msg = result == null ? "无" : string.Format("{0}，{1}", name, telephone);
                Console.WriteLine("行号[{0}]：{1}", excelRowNumber, msg);
                return result;
            }
            catch (Exception ex)
            {
                Console.WriteLine("行号[{0}]：查询结果解析异常：{1}", excelRowNumber, ex.Message);
                return null;
            }
            finally
            {
                Interlocked.Increment(ref _responseCount); // 原子操作：计数器
            }
        }

        // 输出文件流
        private void OutputFileStream()
        {
            using (FileStream fileStream = File.Open(this._excelPath, FileMode.OpenOrCreate, FileAccess.ReadWrite))
            {
                _workbook.Write(fileStream);
                fileStream.Close();
            }
        }

        // 初始化workbook
        private void InitWorkbook()
        {
            try
            {
                using (FileStream file = File.OpenRead(this._excelPath))
                {
                    if (this._excelPath.IndexOf(".xlsx") > 0) // 2007版本
                        _workbook = new XSSFWorkbook(file);
                    else if (this._excelPath.IndexOf(".xls") > 0) // 2003版本
                        _workbook = new HSSFWorkbook(file);
                    file.Close();
                }
            }
            catch (IOException ex)
            {
                string errorMsg = string.Format("The process cannot access the file '{0}' because it is being used by another process.", _excelPath);
                if (ex.Message.Equals(errorMsg))
                {
                    Console.WriteLine("读取Excel文件异常：程序执行期间，请务必关闭该excel文件，否则会执行出错");
                }
                throw;
            }

        }

        // 请求入队列
        private void RequestEnqueue(ISheet sheet)
        {
            for (int i = this._fromRowNumber; i <= sheet.LastRowNum; i++)
            {
                IRow row = sheet.GetRow(i);
                //row.GetCell(12) = FirstName
                //row.GetCell(13) = LastName
                //row.GetCell(14) = Telephone

                // 如果表格中当前行的电话号一栏有值，说明上次已查询过，请求队列将排除这一行
                if (row.GetCell(14) != null && !string.IsNullOrWhiteSpace(row.GetCell(14).ToString())) continue;

                for (int j = 10; j < 16; j++)
                {
                    if (row.GetCell(j) == null) row.CreateCell(j);
                }

                _requestQueue.Enqueue(new RequestInfo()
                {
                    ExcelRowNumber = i,
                    StreetNumber = row.GetCell(0).ToString(),
                    StreetName = row.GetCell(2).ToString(),
                    StreetType = row.GetCell(3).ToString(),
                    City = row.GetCell(7).ToString()
                });
            }
            _requestCount = _requestQueue.Count;
        }

        // 响应入队列
        private void ResponseEnqueue(object requestInfo)
        {
            RequestInfo request = requestInfo as RequestInfo;
            string[] response = GetResponseInfo(request.Url, request.ExcelRowNumber);
            if (response != null)
            {
                // response[0:firstName, 1:secondName, 2:telephone]
                _responseQueue.Enqueue(new ResponseInfo(request.ExcelRowNumber, response[0], response[1], response[2]));
            }
        }

        // 输出结果：专门负责写的线程执行该方法
        private void ProcessWriting(ISheet sheet)
        {
            int[] rowNumberArray = new int[_configParams.BatchSaveResultCount];
            //int remainder = _requestCount % SAVE_RECORD_COUNT;
            while (true)
            {
                // 所有请求都已响应，把最后剩下的response回写到excel
                if (_responseCount == _requestCount)
                {
                    int remainingResponse = _responseQueue.Count;
                    if (remainingResponse == 0)
                    {
                        Console.WriteLine(LINE);
                        Console.WriteLine("程序已执行完毕，请退出！");
                        break;
                    }

                    int[] remainingNumberArray = new int[remainingResponse];
                    for (int i = 0; i < remainingResponse; i++)
                    {
                        ResponseInfo response = null;
                        bool success = _responseQueue.TryDequeue(out response);
                        if (success)
                        {
                            IRow row = sheet.GetRow(response.ExcelRowNumber);
                            row.GetCell(12).SetCellValue(response.FirstName);
                            row.GetCell(13).SetCellValue(response.LastName);
                            row.GetCell(14).SetCellValue(response.Telephone);

                            remainingNumberArray[i] = response.ExcelRowNumber;
                        }
                    }
                    OutputFileStream();
                    Console.WriteLine(LINE);
                    Console.WriteLine("=>最后{0}条查询结果已保存到excel{1}行号[{2}]", remainingResponse, Environment.NewLine, string.Join("，", remainingNumberArray));
                    Console.WriteLine(LINE);
                    Console.WriteLine("=>程序已执行完毕，请退出！");
                    break;
                }
                else if (_responseQueue.Count >= _configParams.BatchSaveResultCount)
                {
                    for (int i = 0; i < _configParams.BatchSaveResultCount; i++)
                    {
                        ResponseInfo response = null;
                        bool success = _responseQueue.TryDequeue(out response);
                        if (success)
                        {
                            IRow row = sheet.GetRow(response.ExcelRowNumber);
                            row.GetCell(12).SetCellValue(response.FirstName);
                            row.GetCell(13).SetCellValue(response.LastName);
                            row.GetCell(14).SetCellValue(response.Telephone);

                            rowNumberArray[i] = response.ExcelRowNumber;
                        }
                    }
                    OutputFileStream();
                    Console.WriteLine(LINE);
                    Console.WriteLine("=>{0}条查询结果已保存到excel{1}行号[{2}]", _configParams.BatchSaveResultCount, Environment.NewLine, string.Join("，", rowNumberArray));
                    Console.WriteLine(LINE);

                    // 写excel的线程睡眠3秒钟，把时间片让给发送请求的线程
                    Thread.Sleep(3 * 1000);
                }

            }
        }
    }

    public class ResponseInfo
    {
        public int ExcelRowNumber;
        public string FirstName;
        public string LastName;
        public string Telephone;

        public ResponseInfo(int excelRowNumber, string firstName, string lastName, string telephone)
        {
            this.ExcelRowNumber = excelRowNumber;
            this.FirstName = firstName;
            this.LastName = lastName;
            this.Telephone = telephone;
        }
    }

    public class RequestInfo
    {
        public int ExcelRowNumber { get; set; }
        public string StreetNumber { get; set; }
        public string StreetName { get; set; }
        public string StreetType { get; set; }
        public string City { get; set; }

        public string Url
        {
            get
            {
                return string.Format("http://411.ca/search/?q={0}+{1}+{2}+{3}&st=reverse",
                StreetNumber,
                StreetName,
                StreetType,
                City);
            }
        }
    }

    public class Query411Result
    {
        public bool IsSuccess { get; set; }
        public string Message { get; set; }
    }

    public class ConfigParams
    {
        private int _batchSendRequestCount = 10; // 1次并发多少请求
        private int _batchSendRequestInterval = 10 * 1000; // 每2次发送请求的时间间隔（单位：毫秒）
        private int _batchSaveResultCount = 20; // 批量响应多少条查询结果保存一次excel

        public int BatchSendRequestCount { get { return _batchSendRequestCount; } }
        public int BatchSendRequestInterval { get { return _batchSendRequestInterval; } }
        public int BatchSaveResultCount { get { return _batchSaveResultCount; } }

        public ConfigParams() { }

        public ConfigParams(int batchSendRequestCount, int batchSendRequestInterval, int batchSaveResultCount)
        {
            this._batchSendRequestCount = batchSendRequestCount;
            this._batchSendRequestInterval = batchSendRequestInterval;
            this._batchSaveResultCount = batchSaveResultCount;
        }
    }
}