using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Integra.EDICDRFileWatcher
{
    /// <summary>
    /// Added by YReddy on 19 Aug 2015
    /// EDI CDR Class to call EDI CDR service when we receives any files from ZirMed
    /// </summary>
    public class EDICDR
    {
        #region Private variables
        //File System Watcher object
        private FileSystemWatcher _edicdrFileWatcher;
        //Cancellation Token object
        private CancellationTokenSource _cancelTokenSrc;
        //Task Factories
        TaskFactory _processFactory = null;

        #region  Blocking Collection for processing EDI CDR files

        /// <summary>
        /// Indicated Collection for Files to be Process
        /// </summary>
        BlockingCollection<string> _fileCollection = null;

        /// <summary>
        /// Indicated Moving file Collection
        /// </summary>
        BlockingCollection<string> _moveFileCollection = null;

        public string logFileName = null;
        #endregion

        #endregion

        #region Properties

        /// <summary>
        /// Maximum retry count.
        /// </summary>
        private static int _maxRetryCount;
        public static int MaxRetryCount { get { return _maxRetryCount; } set { _maxRetryCount = value; } }

        /// <summary>
        /// Maximum delay for retry in milliseconds.
        /// </summary>
        private static int _maxDelayForRetry;
        public static int MaxDelayForRetry { get { return _maxDelayForRetry; } set { _maxDelayForRetry = value; } }
              
        /// <summary>
        /// Source Folder Path
        /// </summary>
        private static string _sourceFolderPath;
        public static string SourceFolderPath { get { return _sourceFolderPath; } set { _sourceFolderPath = value; } }
        /// <summary>
        /// Files to Pick with Pick up Filter
        /// </summary>
        public static string FileFilter = string.Empty;
        /// <summary>
        /// EDI CDR Exe Path to run after files received
        /// </summary>
        public static string EDICDRExePath = string.Empty;
        /// <summary>
        /// LOG Path 
        /// </summary>
        public static string LOGPath = string.Empty;
        public static int MaxServiceCalls = 2;
        #endregion

        #region Constructor
        public EDICDR()
        {
            var assemblyDirectory = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            LogHelper.WriteTraceLog(string.Format("Assembly Path {0}", assemblyDirectory), LogLevel.Debug);

            //Setting Source and Error folders paths 
            _sourceFolderPath = Convert.ToString(ConfigurationManager.AppSettings["SourceFolder"]);
            if (_sourceFolderPath.EndsWith(@"\"))
                _sourceFolderPath = _sourceFolderPath.Remove(_sourceFolderPath.Length - 1, 1);

            EDICDRExePath = ConfigurationManager.AppSettings["EDICDRExePath"];
            FileFilter = ConfigurationManager.AppSettings["SourceFileTypeFilter"];
            LOGPath = System.Configuration.ConfigurationManager.AppSettings["LOGPath"];

            //Its specify number of Retries to be done when file has error json Data
            _maxRetryCount = Convert.ToInt32(ConfigurationManager.AppSettings["MaxRetryCount"]);
            _maxDelayForRetry = Convert.ToInt32(ConfigurationManager.AppSettings["RetryInverval"]);
            LogHelper.WriteTraceLog(string.Format("EDI CDR watcher with details : SourceFolderPath: {0}", SourceFolderPath), LogLevel.Information);
        }
        #endregion

        #region Public & Private methods

        /// <summary>
        /// Starts the folder Watcher
        /// </summary>
        /// <param name="folderPath">Source folder path </param>
        /// <param name="fileFilter">File filter for watch e.g. *.txt, etc</param>
        public void StartFolderWatcher(string folderPath=null)
        {
            try
            {
                if (folderPath == null)
                    folderPath = SourceFolderPath;

                string sLogfile = "EDI_CDR_FileWatcher_LOG_" + DateTime.Now.ToString("yyyyMMdd") + ".log";
                logFileName = LOGPath + "\\" + sLogfile;
                LogHelper.WriteTraceLog(string.Format("Starting EDI CDR Folder Watcher on Folder Path:{0} and Filter type: {1}, Log File: {2}"
                                        , folderPath, FileFilter, logFileName), LogLevel.Debug);
                var dirInfo = new DirectoryInfo(folderPath);
                var fileCollection = dirInfo.GetFiles(FileFilter);

                // Initializing File Watcher object
                _cancelTokenSrc = new CancellationTokenSource();
                _edicdrFileWatcher = new FileSystemWatcher(folderPath, FileFilter);
                _edicdrFileWatcher.NotifyFilter = NotifyFilters.FileName | NotifyFilters.CreationTime;
                _edicdrFileWatcher.InternalBufferSize = 65536;
                _edicdrFileWatcher.EnableRaisingEvents = true;
                _edicdrFileWatcher.Created += new FileSystemEventHandler(_edicdrFileWatcher_Created);

                #region Blocking Collection implementation

                //Create process factory for process  in different stage
                _processFactory = new TaskFactory(TaskCreationOptions.LongRunning, TaskContinuationOptions.None);
                _fileCollection = new BlockingCollection<string>(100);
                _moveFileCollection = new BlockingCollection<string>(100);

                //Read FileName from Watcher and notice to Process
                _processFactory.StartNew(() => { ProcessCDRfiles(); }, _cancelTokenSrc.Token);
                #endregion

                #region Processing avaialable files in source folder when service started
                var remainingFilesTask = Task.Factory.StartNew(() =>
                {
                    if (fileCollection.Any())
                    {
                        LogHelper.WriteTraceLog("Source folder already have files to Process.So remaining files processing Task started.", LogLevel.Information);
                        Parallel.ForEach(fileCollection, file =>
                        {
                            //Adding Remaining files to FileCollections 
                            _fileCollection.Add(file.FullName);
                        });
                        LogHelper.WriteTraceLog("Remaining files processing Task finished.", LogLevel.Information);
                    }                    
                }, _cancelTokenSrc.Token);
                #endregion
                
                LogHelper.WriteTraceLog(string.Format("EDI CDR service to be executed is located at: {0}", EDICDRExePath), LogLevel.Debug);
                LogHelper.WriteTraceLog("Successfully EDI CDR file Watcher service Started.\n", LogLevel.Debug);
            }
            catch (Exception ex)
            {
                LogHelper.WriteTraceLog("EDI CDR Service Folder Watcher Error while starting.", ex, LogLevel.Error);
                throw ex;
            }
        }

        /// <summary>
        /// Stop the Folder Watcher
        /// </summary>
        public void StopFolderWatcher()
        {
            try
            {
                if (_cancelTokenSrc == null)
                    _cancelTokenSrc = new CancellationTokenSource();
                if (_edicdrFileWatcher == null)
                    _edicdrFileWatcher = new FileSystemWatcher();

                _cancelTokenSrc.Token.ThrowIfCancellationRequested();
                _edicdrFileWatcher.EnableRaisingEvents = false;
                _edicdrFileWatcher.Created -= new FileSystemEventHandler(_edicdrFileWatcher_Created);
                LogHelper.WriteTraceLog("EDI CDR file Watcher service stopped.\n", LogLevel.Debug);
            }
            catch (Exception ex)
            {
                LogHelper.WriteTraceLog("EDI CDR file Watcher service stopped with Error.\n", ex, LogLevel.Error);
                throw ex;
            }
        }

        /// <summary>
        /// Used to Read the fileName from File Watcher folder
        /// </summary>
        private void ProcessCDRfiles()
        {
            int calls = 0;
            foreach (var file in _fileCollection.GetConsumingEnumerable())
            {
                if (_cancelTokenSrc.IsCancellationRequested)
                    break;

                var dirInfo = new DirectoryInfo(SourceFolderPath);
                var fileCollections = dirInfo.GetFiles(FileFilter);
                int attempts = 0; 
                while (true && fileCollections.Any() && (MaxServiceCalls > calls))
                {
                    try
                    {   
                        LogHelper.WriteTraceLog(string.Format("Start running EDI CDR service located at: {0}", EDICDRExePath), LogLevel.Debug);
                        //Running the EDI CDR files if any files are available                        
                        System.Diagnostics.Process.Start(EDICDRExePath);
                        LogHelper.WriteTraceLog(string.Format("EDI CDR EXE successfully Called and Processed files.\n"), LogLevel.Debug);
                        calls++;
                        Thread.Sleep(MaxDelayForRetry);                        
                        break;
                    }
                    catch (Exception ex)
                    {
                        if (attempts > MaxRetryCount)
                        {
                            string errordescription = string.Format("Error occurred while Reading FilName from File Path:{0}.", file);
                            LogHelper.WriteTraceLog(errordescription, ex, LogLevel.Error);
                            break;
                        }
                        attempts++;
                        //LogHelper.WriteTraceLog("Retry to read/open message file again, because failed to read/open message file.", LogLevel.Information);
                    }
                }
            }
        }

        #endregion

        #region File/Folder Watcher events

        /// <summary>
        /// This event will fired when new is created under File watcher Folder.
        /// </summary>
        /// <param name="sender">object </param>
        /// <param name="e">File System EventArgs </param>
        void _edicdrFileWatcher_Created(object sender, FileSystemEventArgs e)
        {
            //Will fired when new file appears in source folder
            if (_fileCollection == null)
                _fileCollection = new BlockingCollection<string>(100);
            LogHelper.WriteTraceLog(string.Format("EDI CDR Source folder have a file with Name: {0}.", e.Name), LogLevel.Information);

            _fileCollection.Add(e.Name);
        }
        #endregion
    }

    #region Logs Features
   
    /// <summary>
    /// Logging every action
    /// </summary>
    public class LogHelper
    {   
        /// <summary>
        /// Logs a message as a specified level
        /// </summary>
        /// <param name="message"></param>
        /// <param name="level"></param>
        public static void WriteTraceLog(string message, LogLevel level)
        {
            WriteTraceLog(message, null, level);
        }
        /// <summary>
        /// Logs an exception as an information as an error
        /// </summary>
        /// <param name="message"></param>
        /// <param name="exception"></param>
        public static void WriteTraceLog(string message, Exception exception)
        {
            WriteTraceLog(message, exception, LogLevel.Error);
        }
        /// <summary>
        /// Logs an exception as specified level
        /// </summary>
        /// <param name="message"></param>
        /// <param name="exception"></param>
        /// <param name="level"></param>
        public static void WriteTraceLog(string message, Exception exception, LogLevel level)
        {
            string sLog = System.Configuration.ConfigurationManager.AppSettings["LOGPath"];
            string sLogfile = "EDI_CDR_FileWatcher_LOG_" + DateTime.Now.ToString("yyyyMMdd") + ".log";
           var logsFileName = sLog + "\\" + sLogfile;

            string messages = string.Format("[{0}] - {1} - {2}  {3}", DateTime.Now.ToString(), level, message, exception);
            using (FileStream fs = new FileStream(logsFileName, FileMode.Append, FileAccess.Write, FileShare.ReadWrite))
            {
                using (StreamWriter StreamWriter = new StreamWriter(fs))
                {
                    StreamWriter.WriteLine(messages);
                    StreamWriter.Close();
                }
            }
        }
    }

    /// <summary>
    /// Used to communicate log details with the Log4Net
    /// </summary> 
    public class LogPacket
    {
        public Exception Exception { get; set; }

        public string ExceptionString { get; set; }

        public string StackTrace { get; set; }

        public LogLevel Level { get; set; }

        public string Message { get; set; }
    }

    /// <summary>
    /// Logging Level's
    /// </summary>
    public enum LogLevel
    {
        Exception = 0,
        Error = 1,
        Warning = 2,
        Information = 3,
        Debug = 4
    }
    #endregion
}
