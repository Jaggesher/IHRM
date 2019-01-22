using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;

namespace OpusIHRM
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                IHRM ihrm = new IHRM();
                DataUpdate dataUpdate = new DataUpdate();

                while (true)
                {

                    Console.WriteLine("Executing...........");

                    ihrm.Run();
                    dataUpdate.Run();

                    Console.WriteLine("\n-------------------------------------------------------------------------------\n");
                    Console.WriteLine("Waiting...........");
                    System.Threading.Thread.Sleep(1 * 20 * 1000);

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Interrapt From Main Thread");
                Console.WriteLine(e.Message);
                Console.ReadLine();
            }

        }
    }

    class IHRM
    {
        private readonly string SourceFolder;
        private readonly string BackupFolder;
        private readonly string DestinationFolder;
        private readonly string ConnectionString;
        private readonly string LogFolder;
        private readonly string RawXMLBackUp;
        private readonly string DuplicateFolder;
        private readonly string ProcessedFolder;
        private readonly SealXmlFile sealXmlFile;
        private readonly XmlDocument doc;

        public IHRM()
        {
            #region Testing...
            //SourceFolder = @"E:\Development\Jogessor\DosExperiment\Source";
            //BackupFolder = @"E:\Development\Jogessor\DosExperiment\BackUp";
            //RawXMLBackUp = @"E:\Development\Jogessor\DosExperiment\RawXMLBackUp";
            //DestinationFolder = @"E:\Development\Jogessor\DosExperiment\Destination";
            //DuplicateFolder = @"E:\Development\Jogessor\DosExperiment\Duplicate";
            //ProcessedFolder = @"E:\Development\Jogessor\DosExperiment\ProcessedFolder";
            //LogFolder = @"E:\Development\Jogessor\DosExperiment\LogFiles\";
            //ConnectionString = @"Data Source=.;Initial Catalog=db_Goldfish;User ID=sa;Password=sa@1234;Pooling=true;Max Pool Size=32700;Integrated Security=True";
            #endregion

            #region Deploy...
            SourceFolder = @"C:\inetpub\wwwroot\Payroll\Upload\XmlData";
            BackupFolder = @"E:\IHRMFiles\BackUp";
            RawXMLBackUp = @"E:\IHRMFiles\RAWXmlBackup";
            DestinationFolder = @"X:\SLR.READ";
            DuplicateFolder = @"E:\IHRMFiles\Duplicate";
            ProcessedFolder = @"E:\IHRMFiles\Processed";
            LogFolder = @"E:\IHRMFiles\LogFiles\";
            ConnectionString = @"Data Source=WIN-AJMS15ULNA8\Ablsql;Initial Catalog=db_Goldfish;User ID=sa;Password=Abl#743%; Pooling=true;Max Pool Size=32700;";
            #endregion

            sealXmlFile = SealXmlFile.getInstance();
            doc = new XmlDocument();
        }
        public void Run()
        {
            try
            {
                string timeStamp = DateTime.Now.ToString("yyyyMMddHHmmssffff");
                string LogFileName = LogFolder + "XMLToRead_" + timeStamp + ".txt";
                FileStream fs = new FileStream(LogFileName, FileMode.CreateNew, FileAccess.Write);
                StreamWriter sw = new StreamWriter(fs);
                sw.WriteLine(DateTime.Now);

                int AffectedFileCount = 0;

                DirectoryInfo desFolder = new DirectoryInfo(DestinationFolder);
                int filesInTemonous = desFolder.GetFiles().Count();
                if (filesInTemonous == 0)
                {
                    if (Directory.Exists(SourceFolder) && Directory.Exists(DestinationFolder))
                    {
                        using (SqlConnection connection = new SqlConnection(ConnectionString))
                        {
                            connection.Open();
                            DirectoryInfo info = new DirectoryInfo(SourceFolder);
                            FileInfo[] files = info.GetFiles().OrderBy(p => p.Name).Take(500).ToArray();
                            foreach (FileInfo file in files)
                            {

                                if (File.Exists(file.FullName))
                                {
                                    sw.Write(file.FullName);
                                    try
                                    {
                                        string AccountNumber, YearMonth, Amount, FileId;
                                        AccountNumber = YearMonth = Amount = FileId = "N/A";
                                        doc.Load(file.FullName);
                                        File.Copy(file.FullName, RawXMLBackUp + "//" + file.Name, true);

                                        XmlNodeList elements = doc.GetElementsByTagName("CREDIT_ACCT_NO");
                                        if (elements.Count > 0)
                                        {
                                            AccountNumber = elements[0].InnerText;
                                        }

                                        elements = doc.GetElementsByTagName("DEBIT_AMOUNT");
                                        if (elements.Count > 0)
                                        {
                                            Amount = elements[0].InnerText;
                                        }

                                        elements = doc.GetElementsByTagName("entereddatetime");
                                        if (elements.Count > 0)
                                        {
                                            YearMonth = elements[0].InnerText;
                                            doc.DocumentElement.RemoveChild(elements[0]);
                                        }

                                        elements = doc.GetElementsByTagName("UniqueId");
                                        if (elements.Count > 0)
                                        {
                                            FileId = elements[0].InnerText;
                                            doc.DocumentElement.RemoveChild(elements[0]);
                                        }

                                        string Tmp = $"SELECT FileName FROM IHRMBatchFileTrack WHERE FileId = '{FileId}';";
                                        SqlCommand cmd = new SqlCommand(Tmp, connection);
                                        string fileIDCheck = (string)cmd.ExecuteScalar();

                                        if (fileIDCheck != null)
                                        {
                                            if (File.Exists(ProcessedFolder + "//" + file.Name)) File.Delete(ProcessedFolder + "//" + file.Name);
                                            File.Move(file.FullName, ProcessedFolder + "//" + file.Name);

                                            Tmp = $" INSERT INTO IHRMGateKeeperLog(FileName, Remarks, DateTime) VALUES('{file.Name}', 'File Blocked With status Duplicate ID', getdate());";
                                            cmd = new SqlCommand(Tmp, connection);
                                            cmd.ExecuteScalar();

                                            continue;
                                        }

                                        Tmp = $"SELECT Status FROM IHRMBatchLog WHERE YearMonth = '{YearMonth}' AND AccountNumber = '{AccountNumber}' AND Amount = '{Amount}';";
                                        cmd = new SqlCommand(Tmp, connection);
                                        string fileStatusCheck = (string)cmd.ExecuteScalar();

                                        if (fileStatusCheck == null || fileStatusCheck == "fail")
                                        {

                                            doc.Save(file.FullName);

                                            File.Copy(file.FullName, DestinationFolder + "\\" + file.Name, true);
                                            sw.Write(" | Coppied  successfully");
                                            if (File.Exists(BackupFolder + "\\" + file.Name)) File.Delete(BackupFolder + "\\" + file.Name);
                                            File.Move(file.FullName, BackupFolder + "\\" + file.Name);
                                            sw.WriteLine(" | Moved successfully");
                                            AffectedFileCount++;

                                            string MyTemmp = fileStatusCheck == null ? "New File" : fileStatusCheck;
                                            if (fileStatusCheck == null)
                                            {
                                                Tmp = $"INSERT INTO IHRMBatchLog (FileName, AccountNumber, YearMonth, Amount, Status, InitialDateTime) VALUES('{file.Name}','{AccountNumber}','{YearMonth}','{Amount}','posted',getdate());";
                                                cmd = new SqlCommand(Tmp, connection);
                                                cmd.ExecuteScalar();
                                            }
                                            else
                                            {
                                                Tmp = $"Update IHRMBatchLog SET FileName = '{file.Name}', Status='posted' WHERE YearMonth = '{YearMonth}' AND AccountNumber = '{AccountNumber}' AND Amount = '{Amount}';";
                                                cmd = new SqlCommand(Tmp, connection);
                                                cmd.ExecuteScalar();
                                            }

                                            Tmp = $"INSERT INTO IHRMBatchFileTrack (FileId, FileName, DateTime) VALUES('{FileId}','{file.Name}',getdate());";
                                            cmd = new SqlCommand(Tmp, connection);
                                            cmd.ExecuteScalar();

                                            Tmp = $" INSERT INTO IHRMGateKeeperLog(FileName, Remarks, DateTime) VALUES('{file.Name}', 'File Pass With status {MyTemmp}', getdate());";
                                            cmd = new SqlCommand(Tmp, connection);
                                            cmd.ExecuteScalar();

                                        }
                                        else if (fileStatusCheck == "success")
                                        {

                                            if (File.Exists(DuplicateFolder + "//" + file.Name)) File.Delete(DuplicateFolder + "//" + file.Name);
                                            File.Move(file.FullName, DuplicateFolder + "//" + file.Name);

                                            Tmp = $" INSERT INTO IHRMGateKeeperLog(FileName, Remarks, DateTime) VALUES('{file.Name}', 'File blocked With status Already Success', getdate());";
                                            cmd = new SqlCommand(Tmp, connection);
                                            cmd.ExecuteScalar();
                                        }
                                    }
                                    catch (IOException e)
                                    {
                                        sw.WriteLine(e.Message);
                                        Console.WriteLine(e.Message);
                                    }

                                }
                                else
                                {
                                    sw.WriteLine("File Cn't Found While moving");
                                }
                            }
                        }
                    }
                    else
                    {
                        sw.WriteLine("Directory Missing..");
                    }
                    Console.WriteLine(DateTime.Now + " | Done.");
                }
                else
                {
                    Console.WriteLine(DateTime.Now + " | Temonous Is Busy.");
                    sw.WriteLine("Temonous In Busy Now");
                }

                DirectoryInfo Temp = new DirectoryInfo(SourceFolder);

                Console.WriteLine("Files In Queue: " + Temp.GetFiles().Count());

                sw.Flush();
                sw.Close();
                fs.Close();

                if (AffectedFileCount == 0) File.Delete(LogFileName);

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

        }
    }

    class DataUpdate
    {
        private readonly string LogFile;
        private readonly string ConnectionString;
        private readonly string sourceFolder;
        private readonly string WriteBackUpFolder;

        public DataUpdate()
        {

            #region Testing... 
            //LogFile = @"E:\Development\Jogessor\DosExperiment\IHRMStatusUpdateLog.txt";
            //ConnectionString = @"Data Source=.;Initial Catalog=db_Goldfish;User ID=sa;Password=sa@1234;Pooling=true;Max Pool Size=32700;Integrated Security=True";
            //sourceFolder = @"E:\Development\Jogessor\DosExperiment\SLR.WRITE";//Assuming Test is your Folder
            //WriteBackUpFolder = @"E:\Development\Jogessor\DosExperiment\WriteBackUpFolder";
            #endregion

            #region Deploy...
            LogFile = @"E:\IHRMFiles\IHRMStatusUpdateLog.txt";
            ConnectionString = @"Data Source=WIN-AJMS15ULNA8\Ablsql;Initial Catalog=db_Goldfish;User ID=sa;Password=Abl#743%; Pooling=true;Max Pool Size=32700;";
            sourceFolder = @"X:\SLR.WRITE";
            WriteBackUpFolder = @"E:\IHRMFiles\WriteBackup";
            #endregion

        }

        public void Run()
        {
            string flag = "success";

            try
            {
                int Count = 0;
                // Connect to SQL
                using (SqlConnection connection = new SqlConnection(ConnectionString))
                {
                    connection.Open();
                    string FileName, TranNumber, Status, ErrMessage;
                    DirectoryInfo d = new DirectoryInfo(sourceFolder);
                    FileInfo[] Files = d.GetFiles("*.xml"); //Getting Text files

                    foreach (FileInfo file in Files)
                    {
                        if (File.Exists(WriteBackUpFolder + "\\" + file.Name)) continue;

                        FileName = file.Name;
                        TranNumber = "N/A";
                        Status = "N/A";
                        ErrMessage = "N/A";

                        string content = File.ReadAllText(file.FullName);

                        string[] contents = content.Split(',');

                        if (contents.Length > 0)
                        {
                            //First TRAN NUmber
                            TranNumber = contents[0];

                            string[] FirstFields = contents[0].Split('/');

                            //Status Code
                            Status = FirstFields[2];

                            if (FirstFields[2] != "1")
                            {
                                //Error Message
                                ErrMessage = contents[1];
                            }

                        }

                        //Console.WriteLine(TranNumber + " "+ Status + " " +ErrMessage);
                        string NormalFileName = Path.GetFileNameWithoutExtension(FileName);

                        //Console.WriteLine(NormalFileName);

                        string Tmp = $"UPDATE TemonusData SET TransactionNo = '{TranNumber}', ErrorMessage = '{ErrMessage}', StatusCode = '{Status}' WHERE FileName = '{NormalFileName}'";
                        SqlCommand cmd = new SqlCommand(Tmp, connection);
                        cmd.ExecuteScalar();
                        File.Copy(file.FullName, WriteBackUpFolder + "\\" + file.Name, true);
                        if (Status == "1")
                        {
                            Tmp = $"UPDATE IHRMBatchLog SET Status = 'success', SuccessDateTime = getdate() WHERE FileName='{file.Name}'";
                            cmd = new SqlCommand(Tmp, connection);
                            cmd.ExecuteScalar();
                        }
                        else
                        {
                            Tmp = $"UPDATE IHRMBatchLog SET Status = 'fail' WHERE FileName='{file.Name}'";
                            cmd = new SqlCommand(Tmp, connection);
                            cmd.ExecuteScalar();
                        }
                        Count++;

                        //Console.WriteLine("I");                                     
                    }
                }
                flag = Count.ToString() + ", Files Updated";
            }
            catch (SqlException e)
            {
                flag = e.ToString();
                //Console.WriteLine(e.ToString());
            }

            try
            {
                FileStream fs = new FileStream(LogFile, FileMode.Append, FileAccess.Write);
                StreamWriter sw = new StreamWriter(fs);
                sw.WriteLine(DateTime.Now + " | " + flag);
                Console.WriteLine(DateTime.Now + " | " + flag);
                sw.Flush();
                sw.Close();
                fs.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
    }

    public class SealXmlFile
    {
        private readonly XmlDocument xmlDoc;
        private static SealXmlFile instance = new SealXmlFile();

        private SealXmlFile()
        {
            xmlDoc = new XmlDocument();
        }

        public string SealXml(string FileFullPath)
        {
            try
            {
                xmlDoc.Load(FileFullPath);
                XmlElement record = xmlDoc.CreateElement("ABL_Opus");
                record.SetAttribute("type", "General Status.");
                record.InnerText = "Posted-" + DateTime.Now;
                xmlDoc.DocumentElement.AppendChild(record);
                xmlDoc.Save(FileFullPath);
                return "Success";
            }
            catch (XmlException e)
            {
                Console.WriteLine("Error from xml seal Operation.");
                Console.WriteLine(e.Message);
                return "Fail";
            }
        }

        public static SealXmlFile getInstance()
        {
            return instance;
        }
    }

}
