using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        private readonly string LogFolder;

        public IHRM()
        {
            //Testing...
            SourceFolder = @"D:\Opus\Development\Jogessor\DosExperiment\Source";
            BackupFolder = @"D:\Opus\Development\Jogessor\DosExperiment\BackUp";
            DestinationFolder = @"D:\Opus\Development\Jogessor\DosExperiment\Destination";
            LogFolder = @"D:\Opus\Development\Jogessor\DosExperiment\LogFiles\";

            //Deploy...
            //SourceFolder = @"C:\inetpub\wwwroot\Payroll\Upload\XmlData";
            //BackupFolder = @"E:\IHRMFiles\BackUp";
            //DestinationFolder = @"X:\SLR.READ";
            //LogFolder = @"E:\IHRMFiles\LogFiles\";


        }
        public void Run()
        {
            try
            {
                string timeStamp = DateTime.Now.ToString("yyyyMMddHHmmssffff");
                FileStream fs = new FileStream(LogFolder + timeStamp + ".txt", FileMode.CreateNew, FileAccess.Write);
                StreamWriter sw = new StreamWriter(fs);
                sw.WriteLine(DateTime.Now);

                DirectoryInfo desFolder = new DirectoryInfo(DestinationFolder);
                int filesInTemonous = desFolder.GetFiles().Count();
                if (filesInTemonous == 0)
                {
                    if (Directory.Exists(SourceFolder) && Directory.Exists(DestinationFolder))
                    {
                        DirectoryInfo info = new DirectoryInfo(SourceFolder);
                        FileInfo[] files = info.GetFiles().OrderBy(p => p.Name).Take(500).ToArray();
                        foreach (FileInfo file in files)
                        {

                            if (File.Exists(file.FullName))
                            {
                                sw.WriteLine(file.FullName);
                                try
                                {
                                    File.Copy(file.FullName, DestinationFolder + "\\" + file.Name, true);
                                    sw.WriteLine("Coppied  successfully");
                                    File.Move(file.FullName, BackupFolder + "\\" + file.Name);
                                    sw.WriteLine("Moved successfully");
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
            //Deploy...

            //LogFile = @"";
            //ConnectionString = @"Data Source=WIN-7HGA9A6FBHT;Initial Catalog=db_ABL_RTGS;User ID=sa;Password=sa@123; Pooling=true;Max Pool Size=32700;";
            //sourceFolder = @"";//Assuming Test is your Folder
            //WriteBackUpFolder = @"D:\Opus\Development\Jogessor\DosExperiment\WriteBackUpFolder";

            //Testing... 
            LogFile = @"D:\Opus\Development\Jogessor\DosExperiment\IHRMStatusUpdateLog.txt";
            ConnectionString = @"Data Source=DESKTOP-ALPFNNL;Initial Catalog=db_Goldfish;User ID=sa;Password=sa@123; Pooling=true;Max Pool Size=32700;";
            sourceFolder = @"D:\Opus\Development\Jogessor\DosExperiment\SLR.WRITE";//Assuming Test is your Folder
            WriteBackUpFolder = @"D:\Opus\Development\Jogessor\DosExperiment\WriteBackUpFolder";
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
}
