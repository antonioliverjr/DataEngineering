using System;
using System.Data;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace Execucao_Procedures
{
    class Program
    {
        static async Task<int> Main(string[] args)
        {
            if (args.Length == 0) return 0;

            SqlConnection sqlconn = new SqlConnection("Server=DELLJR\\SQLEXPRESS;Database=GOVBR;User ID=project;Password=Jrdbsql");

            string procedure = args[0];

            await sqlconn.OpenAsync();
            SqlCommand query = new SqlCommand(procedure, sqlconn);
            query.CommandType = CommandType.StoredProcedure;
            try
            {
                await query.ExecuteReaderAsync();
            }
            catch
            {
                return 0;
            }

            await sqlconn.CloseAsync();
            return 1;
        }
    }
}
