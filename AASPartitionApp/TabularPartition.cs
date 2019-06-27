using System;
using System.Configuration;
using Newtonsoft.Json;
using System.IO;
using Microsoft.AnalysisServices.Tabular;
using System.Security;

namespace AASPartitionApp
{
    class TabularPartition
    {
        static void Main(string[] args)
        {
            try
            {
                DateTime startTime = DateTime.Now;

                //Get Extended Properties from activity.json which is created by ADF in same folder as this exe at runtime
                Console.WriteLine("Initializing variables from activity.json");
                dynamic activity = JsonConvert.DeserializeObject(File.ReadAllText("activity.json"));
                string instance = activity.typeProperties.extendedProperties.Instance;
                string database = activity.typeProperties.extendedProperties.Database;
                string table = activity.typeProperties.extendedProperties.Table;
                string partitionName = activity.typeProperties.extendedProperties.PartitionName;
                string partitionDatasource = activity.typeProperties.extendedProperties.PartitionDatasource;
                string partitionQuery = activity.typeProperties.extendedProperties.PartitionQuery;

                //Get App.config
                Console.WriteLine("Initializing variables from app.config");
                string userID = ConfigurationManager.AppSettings["userID"];
                string password = ConfigurationManager.AppSettings["encryptedPassword"];

                string connectionString = "Provider=MSOLAP" +
                                    ";Data Source=" + instance +
                                    ";Initial Catalog=" + database +
                                    ";User ID=" + userID +
                                    ";Password=" + password +
                                    ";Persist Security Info=True;Impersonation Level=Impersonate";

                Console.WriteLine("Initializing TabularPartition class");
                TabularPartition tabpartition = new TabularPartition(connectionString, database, table);

                if (args.Length > 0)
                {
                    switch (args[0].ToString())
                    {
                        case "create":
                            Console.WriteLine("Creating partition:" + partitionName);
                            tabpartition.ActionPartition(actionType.CREATE,partitionName, partitionQuery, partitionDatasource);
                            Console.WriteLine("Partition created");
                            break;
                        case "delete":
                            Console.WriteLine("Deleting partition:" + partitionName);
                            tabpartition.ActionPartition(actionType.DELETE, partitionName);
                            Console.WriteLine("Partition deleted");
                            break;
                        case "process":
                            Console.WriteLine("Processing partition:" + partitionName);
                            tabpartition.ActionPartition(actionType.PROCESS, partitionName);
                            Console.WriteLine("Partition processed");
                            break;
                        default:
                            break;
                    }

                }


                DateTime endTime = DateTime.Now;
                Console.WriteLine("Activity completed in " + (endTime-startTime).Seconds.ToString() + " seconds");

            }
            catch (Exception ex)
            {
                Console.WriteLine("Process failed with exception " + ex.Message);
                throw;
            }
        }


        public string server { get; set; }
        public string database { get; set; }
        public string table { get; set; }
        public enum actionType { CREATE,DELETE,PROCESS};

        //default constructor
        public TabularPartition(){ }

        //overloaded constructors
        public TabularPartition (string AASServer, string AASDatabase, string AASTable)
        {
            server = AASServer;
            database = AASDatabase;
            table = AASTable;
        }

        public void ActionPartition(actionType action, string partitionName, string partitionQuery = "", string partitionDatasource = "")
        {
            try
            {
                Server tabServer = new Server();
                tabServer.Connect(server);
                Database tabDatabase = tabServer.Databases.GetByName(this.database);

                Table tabTable = tabDatabase.Model.Tables.Find(this.table);

                switch (action)
                {
                    case actionType.CREATE:
                         if (!tabTable.Partitions.ContainsName(partitionName))
                          {
                                Partition tabPartition = new Partition()
                                {
                                    Name = partitionName,
                                    Source = new QueryPartitionSource()
                                    {
                                        DataSource = tabTable.Model.DataSources.Find(partitionDatasource),
                                        Query = partitionQuery
                                    }
                                };

                                tabTable.Partitions.Add(tabPartition);
                                tabDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                        }
                        break;
                    case actionType.DELETE:
                        if (tabTable.Partitions.ContainsName(partitionName))
                         {
                                tabTable.Partitions.Remove(partitionName);
                                tabDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                        }
                        break;
                    case actionType.PROCESS:
                        if (tabTable.Partitions.ContainsName(partitionName))
                        {
                            
                            Partition tabPartition = tabTable.Partitions.Find(partitionName);
                            tabPartition.RequestRefresh(RefreshType.Full);
                            tabDatabase.Update(Microsoft.AnalysisServices.UpdateOptions.ExpandFull);
                        }
                        break;
                    default:
                        break;
                }

                tabServer.Disconnect();
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception Raised from: " + ex.Source);
                Console.WriteLine("Error Message: " + ex.Message);
                Console.WriteLine("Stack Trace:" + ex.StackTrace);
                throw ex;
            }
            
        }
    
    }
}
