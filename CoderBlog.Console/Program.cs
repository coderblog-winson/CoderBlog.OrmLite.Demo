using ServiceStack.OrmLite;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;

namespace CoderBlog.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                //get the connection string and other settings from app.config
                var connection = ConfigurationManager.ConnectionStrings["Default"].ConnectionString;

                var isUpdateAll = Convert.ToBoolean(ConfigurationManager.AppSettings["UpdateAll"]);

                var updateTables = ConfigurationManager.AppSettings["UpdateTables"].Split(',').ToList();

                var nameSpace = ConfigurationManager.AppSettings["ModelNamespace"];

                //load the assembly for dynamic to load model
                var asm = Assembly.Load(nameSpace);

                //dynamic get the models by namespace
                var models = asm.GetTypes().Where(p =>
                     p.Namespace == nameSpace
                ).ToList();

                List<object> objects = new List<object>();
                foreach (var model in models)
                {
                    objects.Add(Activator.CreateInstance(model));
                }

                //create the db factory with OrmLite
                var dbFactory = new OrmLiteConnectionFactory(connection, SqlServerDialect.Provider);

                using (var db = dbFactory.OpenDbConnection())
                {
                    using (IDbTransaction trans = db.OpenTransaction(IsolationLevel.ReadCommitted))
                    {
                        foreach (var o in objects)
                        {
                            var model = o.GetType();

                            if (isUpdateAll || (updateTables.Where(t => t == model.Name).Any() && !isUpdateAll))
                            {
                                //dynamic to call the UpdateTable method so that can support all models
                                Migration m = new Migration();                                
                                MethodInfo method = typeof(Migration).GetMethod("UpdateTable");
                                MethodInfo generic = method.MakeGenericMethod(model);
                                generic.Invoke(m, new object[] { db, new MSSqlProvider() });
                            }
                        }
                        trans.Commit();
                    }
                }

                System.Console.WriteLine("Database has been updated!");
                System.Console.Read();
            }
            catch(Exception ex)
            {
                System.Console.WriteLine("Error: " + ex.Message);
                System.Console.Read();
                //throw ex;
            }
        }
    }

    /// <summary>
    /// Interface for Sql provider, you can implement it for your custom provider
    /// </summary>
    public interface ISqlProvider
    {
        /// <summary>
        /// Generate drop FK and create FK sql and temp table for migrate the table data
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="currentName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        FKStatement MigrateTableSql(IDbConnection connection, string currentName, string newName);

        string GetColumnNamesSql(string tableName);

        string InsertIntoSql(string intoTableName, string fromTableName, string commaSeparatedColumns);
    }

    /// <summary>
    /// Do the data migration
    /// </summary>
    public class Migration
    {
        /// <summary>
        /// Update table structure by model
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connection"></param>
        /// <param name="sqlProvider"></param>
        public void UpdateTable<T>(IDbConnection connection, ISqlProvider sqlProvider) where T : new()
        {
            try
            {
                connection.CreateTableIfNotExists<T>();

                var model = ModelDefinition<T>.Definition;
                string tableName = model.Name;  //the original table
                string tableNameTmp = tableName + "Tmp"; //temp table for save the data

                //get the existing table's columns
                string getDbColumnsSql = sqlProvider.GetColumnNamesSql(tableName);
                var dbColumns = connection.SqlList<string>(getDbColumnsSql);

                //insert the data to a temp table first
                var fkStatement = sqlProvider.MigrateTableSql(connection, tableName, tableNameTmp);
                connection.ExecuteNonQuery(fkStatement.DropStatement);

                //create a new table
                connection.CreateTable<T>();

                //handle the foreign keys
                if (!string.IsNullOrEmpty(fkStatement.CreateStatement))
                {
                    connection.ExecuteNonQuery(fkStatement.CreateStatement);
                }

                //get the new table's columns
                string getModelColumnsSql = sqlProvider.GetColumnNamesSql(tableName);
                var modelColumns = connection.SqlList<string>(getModelColumnsSql);

                //dynamic get columns from model
                List<string> activeFields = dbColumns.Where(dbColumn => modelColumns.Contains(dbColumn)).ToList();

                //move the data from temp table to new table, so that we can keep the original data after migration
                string activeFieldsCommaSep = string.Join("," , activeFields);
                string insertIntoSql = sqlProvider.InsertIntoSql(tableName, "#temp", activeFieldsCommaSep);

                connection.ExecuteSql(insertIntoSql);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }

    /// <summary>
    /// For generate SQL string for drop and re-recreate foreign keys 
    /// </summary>
    public class FKStatement
    {
        public string ParentObject { get; set; }
        public string ReferenceObject { get; set; }
        public string DropStatement { get; set; }
        public string CreateStatement { get; set; }
    }

    /// <summary>
    /// MSSQL provider
    /// </summary>
    public class MSSqlProvider : ISqlProvider
    {
        /// <summary>
        /// Generate migration SQL, base on individual Database, so we need to handle this by difference provider
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="currentName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public FKStatement MigrateTableSql(IDbConnection connection, string currentName, string newName)
        {
            var fkStatement = new FKStatement();
            //get the drop and re-create foreign keys sqls
            var sql_get_foreign_keys = @"SELECT OBJECT_NAME(fk.parent_object_id) ParentObject, 
                    OBJECT_NAME(fk.referenced_object_id) ReferencedObject,
                    'ALTER TABLE ' + s.name + '.' + OBJECT_NAME(fk.parent_object_id)
                        + ' DROP CONSTRAINT ' + fk.NAME + ' ;' AS DropStatement,
                    'ALTER TABLE ' + s.name + '.' + OBJECT_NAME(fk.parent_object_id)
                    + ' ADD CONSTRAINT ' + fk.NAME + ' FOREIGN KEY (' + COL_NAME(fk.parent_object_id, fkc.parent_column_id)
                        + ') REFERENCES ' + ss.name + '.' + OBJECT_NAME(fk.referenced_object_id)
                        + '(' + COL_NAME(fk.referenced_object_id, fkc.referenced_column_id) + ');' AS CreateStatement
                FROM
                    sys.foreign_keys fk
                INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
                INNER JOIN sys.schemas s ON fk.schema_id = s.schema_id
                INNER JOIN sys.tables t ON fkc.referenced_object_id = t.object_id
                INNER JOIN sys.schemas ss ON t.schema_id = ss.schema_id
                WHERE
                    OBJECT_NAME(fk.referenced_object_id) = '" + currentName + "' AND ss.name = 'dbo';";

            var fkSql = connection.SqlList<FKStatement>(sql_get_foreign_keys);
            if (fkSql.Count > 0)
            {
                foreach (var fk in fkSql)
                {
                    fkStatement.DropStatement += fk.DropStatement;
                    if (fk.ParentObject != currentName)
                    {
                        fkStatement.CreateStatement += fk.CreateStatement;
                    }
                }
            }

            fkStatement.DropStatement += " select * into #temp from (select * from [" + currentName + "]) as tmp; drop table [" + currentName + "]; ";
            return fkStatement;
        }

        /// <summary>
        /// Get the table's columns
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public string GetColumnNamesSql(string tableName)
        {
            return "SELECT name FROM syscolumns  WHERE id = OBJECT_ID('" + tableName + "');";
        }

        /// <summary>
        /// Insert data to new table, for MSSQL server 2008 above, I will disable all CONSTRAINT before insert data and enable them after done.
        /// </summary>
        /// <param name="intoTableName"></param>
        /// <param name="fromTableName"></param>
        /// <param name="commaSeparatedColumns"></param>
        /// <returns></returns>
        public string InsertIntoSql(string intoTableName, string fromTableName, string commaSeparatedColumns)
        {
            return "EXEC sp_msforeachtable \"ALTER TABLE ? NOCHECK CONSTRAINT all\"; SET IDENTITY_INSERT [" + intoTableName + "] ON; INSERT INTO [" + intoTableName + "] (" +
                commaSeparatedColumns + ") SELECT " + commaSeparatedColumns + " FROM [" + fromTableName + "]; SET IDENTITY_INSERT [" + intoTableName + "] OFF;  drop table [" + fromTableName + "];EXEC sp_msforeachtable \"ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all\"";
        }
    }

    /// <summary>
    /// I didn't try this provider, just for the sample, you can try and improve it if you need.
    /// </summary>
    public class MySqlProvider : ISqlProvider
    {
        public FKStatement MigrateTableSql(IDbConnection connection, string currentName, string newName)
        {
            var fkStatement = new FKStatement();
            fkStatement.DropStatement = "RENAME TABLE `" + currentName + "` TO `" + newName + "`;";
            return fkStatement;
        }

        public string GetColumnNamesSql(string tableName)
        {
            return "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "';";
        }

        public string InsertIntoSql(string intoTableName, string fromTableName, string commaSeparatedColumns)
        {
            return "INSERT INTO `" + intoTableName + "` (" + commaSeparatedColumns + ") SELECT " + commaSeparatedColumns + " FROM `" + fromTableName + "`;";
        }

        public string DropTableSql(string tableName)
        {
            return "DROP TABLE `" + tableName + "`;";
        }
    }
}
