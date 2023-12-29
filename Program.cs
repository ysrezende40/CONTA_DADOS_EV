using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ContaDadosEv
{
    class Program
    {

        public string connectionOracle => Properties.Settings.Default.CSO; //tornar o campo somente leitura
        string connectionServer => Properties.Settings.Default.CSS;
        SqlConnection conexaoSQL = null;
        List<string> nrDocAusentes = new List<string>();
       


        static async Task Main()
        {

            Console.WriteLine("Carregando.....: ");
            Program programa = new Program();
              Console.WriteLine("Calculando dados");
            programa.CalculaDados()
            programa.VerificaRegistros();
            Console.WriteLine("Inserindo dados");
            programa.Inseredados();







            Console.WriteLine("Concluido");
        }


        public void CalculaDados()
        {
            int contagemOracle = 0;
            int contagemSQL = 0;

           using (OracleConnection conexaoOracle = new OracleConnection(connectionOracle))
           {
               try
               {
                   conexaoOracle.Open();

                   string query = "select count(DISTINCT NR_DOC) from ev_doc_lib WHERE DT_ATRACACAO >= ADD_MONTHS(TRUNC(SYSDATE), -12)";
                   using (OracleCommand comandoOracle = new OracleCommand(query, conexaoOracle))
                   {
                       contagemOracle = Convert.ToInt32(comandoOracle.ExecuteScalar());

                       Console.WriteLine("Contagem oracle:" + contagemOracle);

                   }
               }
               catch (Exception error)
               {
                   Console.BackgroundColor = ConsoleColor.Red;
                   Console.WriteLine("ERRO" + error);
               }




           }
           using (conexaoSQL = new SqlConnection(connectionServer))
           {


               try
               {
                   conexaoSQL.Open();

                   string query = @"
                   SELECT COUNT(DISTINCT NR_DOC) AS TOTAL FROM EV_DOC_LIB_SERVER 
                   WHERE DT_ATRACACAO >= DATEADD(MONTH, -12, GETDATE())";

                   using (SqlCommand comandoSQL = new SqlCommand(query, conexaoSQL))
                   {
                       contagemSQL = Convert.ToInt32(comandoSQL.ExecuteScalar());
                       Console.WriteLine("Contagem SQL:" + contagemSQL);
                   }
               }
               catch (Exception error)
               {
                   Console.BackgroundColor = ConsoleColor.Red;
                   Console.WriteLine("ERRO" + error);
               }


           }

           int quantofalta = contagemOracle - contagemSQL;
           string resultadoFormatado = (quantofalta / 1000.0).ToString("0.000");
           resultadoFormatado = resultadoFormatado.Replace(",", ".");

           Console.WriteLine($"Falta inserir {resultadoFormatado} registros");


           RegistrarLog($"Operação realizada no dia: {DateTime.Now}");
           RegistrarLog("Contagem SQL:" + contagemSQL);
           RegistrarLog("Contagem Oracle:" + contagemOracle);
           RegistrarLog("Falta inserir " + resultadoFormatado + " registros");
           RegistrarLog("--------------------------------------------");

           Console.ReadKey();

        }

        public void RegistrarLog(string mensagem)
        {
            string path = "C:\\Users\\ysaac.queiroz\\Documents\\RegistraColetaDados\\RegistraContaDados";

            try
            {
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(path, true))
                {
                    file.WriteLine(mensagem);

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Erro ao registrar log: " + ex.Message);
            }


        }


        public void UpdateNR_DOC(string nrDoc)
        {
            using (OracleConnection conexaoOracleUP = new OracleConnection(connectionOracle))
            {
                try
                {
                    conexaoOracleUP.Open();

                    string queryOracle = @"
                                UPDATE EV_DOC_LIB 
                                SET FRETE_USD = ROUND(FRETE_USD, 2) 
                                WHERE NR_DOC = :nrDoc";

                    OracleCommand comandoOracle = new OracleCommand(queryOracle, conexaoOracleUP);
                    comandoOracle.Parameters.Add(new OracleParameter("nrDoc", nrDoc));

                    int rowsAffected = comandoOracle.ExecuteNonQuery();

                    if (rowsAffected > 0)
                    {
                       
                    }
                    else
                    {
                        Console.WriteLine("Nenhum registro foi atualizado.");
                    }
                }
                catch (Exception error)
                {
                    Console.WriteLine("ERRO NO UPDATE: " + error);
                    Console.ReadKey();
                }
            }
        }


        public void VerificaRegistros()
        {
            Console.WriteLine("Verificando registros");
            int limiteLista = 1000;
            int contador = 0;

            using (OracleConnection conexaoOracleVF = new OracleConnection(connectionOracle))
            {
                try
                {
                    conexaoOracleVF.Open();

                    string queryOracle = "SELECT DISTINCT NR_DOC FROM ev_doc_lib WHERE DT_ATRACACAO >= ADD_MONTHS(TRUNC(SYSDATE), -12)";




                    OracleCommand comandoOracle = new OracleCommand(queryOracle, conexaoOracleVF);
                    OracleDataReader Leitordedados = comandoOracle.ExecuteReader();

                    while (Leitordedados.Read())
                    {
                        string NR_DOC = Leitordedados["NR_DOC"].ToString();



                       


                        using (conexaoSQL = new SqlConnection(connectionServer))
                        {
                            conexaoSQL.Open();


                            try
                            {
                                string queryVerificacao = @"
                                 SELECT COUNT(DISTINCT NR_DOC) FROM EV_DOC_LIB_SERVER 
                                 WHERE NR_DOC = @NR_DOC
                                 AND CONVERT(DATE, DT_ATRACACAO) >= CONVERT(DATE, DATEADD(MONTH, -12, GETDATE()))";


                                SqlCommand comandoVerificacao = new SqlCommand(queryVerificacao, conexaoSQL);
                                comandoVerificacao.Parameters.AddWithValue("@NR_DOC", NR_DOC);

                                int count = Convert.ToInt32(comandoVerificacao.ExecuteScalar());

                                if (count == 0)
                                {
                                    Console.ForegroundColor = ConsoleColor.DarkYellow;
                                    Console.WriteLine($"O registro {NR_DOC} não existe no banco de dados");
                                    nrDocAusentes.Add(NR_DOC);
                                    contador++;

                                    if (contador >= limiteLista)
                                    {

                                        Inseredados();
                                        nrDocAusentes.Clear();
                                        contador = 0;
                                    }

                                }
                                else
                                {
                                    Console.WriteLine($"O registro {NR_DOC} ja existe no banco de dados");
                                }
                            }
                            catch (Exception error)
                            {
                                Console.BackgroundColor = ConsoleColor.Red;
                                Console.WriteLine("ERRO: " + error);
                                break;
                            }




                        }

                    }


                    if (nrDocAusentes.Count > 0)
                    {
                        Inseredados();
                    }

                    Console.WriteLine("FIM");
                    Console.ReadKey();
                   
                }
                catch (Exception error)
                {
                    Console.BackgroundColor = ConsoleColor.Red;
                    Console.WriteLine("ERRO: " + error);
                    Console.ReadKey();

                }


            }
        }


        public void Inseredados()
        {
            int registros = 0;


            Console.WriteLine("Começou a inserção");
            using (OracleConnection conexaoOracle = new OracleConnection(connectionOracle))
            {
                conexaoOracle.Open();



                foreach (string nrDocAusente in nrDocAusentes)
                {

                    string queryOracle = $"SELECT DISTINCT * FROM EV_DOC_LIB WHERE NR_DOC = :nrDoc";

                    UpdateNR_DOC(nrDocAusente); //para arredondar os dados e evitar numeros grandes. 

                    using (OracleCommand comandoOracle = new OracleCommand(queryOracle, conexaoOracle))
                    {
                        comandoOracle.Parameters.Add(new OracleParameter("nrDoc", nrDocAusente));

                        using (OracleDataReader Leitordedados = comandoOracle.ExecuteReader())
                        {

                            while (Leitordedados.Read())
                            {


                                string NR_DOC = Leitordedados["NR_DOC"] != DBNull.Value ? Leitordedados["NR_DOC"].ToString() : "";
                                string CD_DECLARACAO = Leitordedados["CD_DECLARACAO"] != DBNull.Value ? Leitordedados["CD_DECLARACAO"].ToString() : "";
                                DateTime DT_DOCUMENTO = Leitordedados["DT_DOCUMENTO"] != DBNull.Value ? Convert.ToDateTime(Leitordedados["DT_DOCUMENTO"]) : DateTime.MaxValue; // se o dado for nulo a data maxima sera inserida
                                DateTime DT_ATRACACAO = Leitordedados["DT_ATRACACAO"] != DBNull.Value ? Convert.ToDateTime(Leitordedados["DT_ATRACACAO"]) : DateTime.MaxValue;
                                DateTime DT_REGISTRO = Leitordedados["DT_REGISTRO"] != DBNull.Value ? Convert.ToDateTime(Leitordedados["DT_REGISTRO"]) : DateTime.MaxValue;
                                DateTime DT_DESEMBARACO = Leitordedados["DT_DESEMBARACO"] != DBNull.Value ? Convert.ToDateTime(Leitordedados["DT_DESEMBARACO"]) : DateTime.MaxValue;
                                DateTime DT_RECEPCAO = Leitordedados["DT_RECEPCAO"] != DBNull.Value ? Convert.ToDateTime(Leitordedados["DT_RECEPCAO"]) : DateTime.MaxValue;
                                string TP_DOCUMENTO = Leitordedados["TP_DOCUMENTO"] != DBNull.Value ? Leitordedados["TP_DOCUMENTO"].ToString() : "";
                                string NUM_DOCUMENTO = Leitordedados["NUM_DOCUMENTO"] != DBNull.Value ? Leitordedados["NUM_DOCUMENTO"].ToString() : "";
                                string TIPO_DOC = Leitordedados["TIPO_DOC"] != DBNull.Value ? Leitordedados["TIPO_DOC"].ToString() : "";
                                string TP_MANIFESTO = Leitordedados["TP_MANIFESTO"] != DBNull.Value ? Leitordedados["TP_MANIFESTO"].ToString() : "";
                                string QTD_ADICOES = Leitordedados["QTD_ADICOES"] != DBNull.Value ? Leitordedados["QTD_ADICOES"].ToString() : "";
                                string RECINTO = Leitordedados["RECINTO"] != DBNull.Value ? Leitordedados["RECINTO"].ToString() : "";
                                string NR_IMPORTADOR = Leitordedados["NR_IMPORTADOR"] != DBNull.Value ? Leitordedados["NR_IMPORTADOR"].ToString() : "";
                                string NOME_IMPORTADOR = Leitordedados["NOME_IMPORTADOR"] != DBNull.Value ? Leitordedados["NOME_IMPORTADOR"].ToString() : "";
                                string TP_IMPORTADOR = Leitordedados["TP_IMPORTADOR"] != DBNull.Value ? Leitordedados["TP_IMPORTADOR"].ToString() : "";
                                string PAIS_ORIGEM = Leitordedados["PAIS_ORIGEM"] != DBNull.Value ? Leitordedados["PAIS_ORIGEM"].ToString() : "";
                                string NOME_PAIS_ORIGEM = Leitordedados["NOME_PAIS_ORIGEM"] != DBNull.Value ? Leitordedados["NOME_PAIS_ORIGEM"].ToString() : "";

                                object valorPB_CARGA = Leitordedados["PB_CARGA"]; // cria um obejto e armazena o dado 
                                float PB_CARGA;

                                if (valorPB_CARGA != DBNull.Value) 
                                {
                                    PB_CARGA = (float)Math.Round(Convert.ToSingle(valorPB_CARGA), 2);

                                }
                                else
                                {
                                    PB_CARGA = 0.0f;
                                }

                                object valorPL_CARGA = Leitordedados["PL_CARGA"];
                                float PL_CARGA;

                                if (valorPL_CARGA != DBNull.Value)
                                {
                                    PL_CARGA = (float)Math.Round(Convert.ToSingle(valorPL_CARGA), 2);

                                }
                                else
                                {
                                    PL_CARGA = 0.0f;
                                }

                                object valorSEGURO_USD = Leitordedados["SEGURO_USD"];
                                float SEGURO_USD;

                                if (valorSEGURO_USD != DBNull.Value)
                                {
                                    SEGURO_USD = (float)Math.Round(Convert.ToSingle(valorSEGURO_USD), 2);

                                }
                                else
                                {
                                    SEGURO_USD = 0.0f;
                                }


                                object valorVAL_DOCUMENTO = Leitordedados["VAL_DOCUMENTO"];
                                float VAL_DOCUMENTO;

                                if (valorVAL_DOCUMENTO != DBNull.Value)
                                {
                                    VAL_DOCUMENTO = (float)Math.Round(Convert.ToSingle(valorVAL_DOCUMENTO), 2);

                                }
                                else
                                {
                                    VAL_DOCUMENTO = 0.0f;
                                }

                                object valorFRETE_USD = Leitordedados["FRETE_USD"];
                                float FRETE_USD = 0;

                                if (valorFRETE_USD != DBNull.Value)
                                {
                                    
                                    string valorString = valorFRETE_USD.ToString().Replace(',', '.');

                                    if (float.TryParse(valorString, System.Globalization.NumberStyles.Any, System.Globalization.CultureInfo.InvariantCulture, out float parsedValue))
                                    {
                                       
                                        FRETE_USD = (float)Math.Round(parsedValue, 2);
                                    }
                                    else
                                    {
                                        Console.WriteLine("Não foi possível converter para float");
                                    }
                                }
                                else
                                {

                                    FRETE_USD = 0.0f;
                                }



                                try
                                {
                                    using (SqlConnection conexaoSQL = new SqlConnection(connectionServer))
                                    {
                                        conexaoSQL.Open();

                                        string queryInsercao = @"INSERT INTO EV_DOC_LIB_SERVER(
                                                                NR_DOC,
                                                                CD_DECLARACAO, 
                                                                DT_DOCUMENTO, 
                                                                DT_ATRACACAO, 
                                                                DT_REGISTRO, 
                                                                DT_DESEMBARACO, 
                                                                DT_RECEPCAO, 
                                                                TP_DOCUMENTO, 
                                                                NUM_DOCUMENTO, 
                                                                TIPO_DOC, 
                                                                TP_MANIFESTO, 
                                                                QTD_ADICOES, 
                                                                RECINTO, 
                                                                NR_IMPORTADOR, 
                                                                NOME_IMPORTADOR, 
                                                                TP_IMPORTADOR, 
                                                                PAIS_ORIGEM, 
                                                                NOME_PAIS_ORIGEM, 
                                                                PB_CARGA, 
                                                                PL_CARGA, 
                                                                FRETE_USD, 
                                                                SEGURO_USD, 
                                                                VAL_DOCUMENTO
                                                            ) VALUES (
                                                                @NR_DOC,
                                                                @CD_DECLARACAO, 
                                                                @DT_DOCUMENTO, 
                                                                @DT_ATRACACAO, 
                                                                @DT_REGISTRO, 
                                                                @DT_DESEMBARACO, 
                                                                @DT_RECEPCAO, 
                                                                @TP_DOCUMENTO, 
                                                                @NUM_DOCUMENTO, 
                                                                @TIPO_DOC, 
                                                                @TP_MANIFESTO, 
                                                                @QTD_ADICOES, 
                                                                @RECINTO, 
                                                                @NR_IMPORTADOR, 
                                                                @NOME_IMPORTADOR, 
                                                                @TP_IMPORTADOR, 
                                                                @PAIS_ORIGEM, 
                                                                @NOME_PAIS_ORIGEM, 
                                                                ROUND(@PB_CARGA, 2), 
                                                                ROUND(@PL_CARGA, 2),
                                                                ROUND(@FRETE_USD, 2),
                                                                ROUND(@SEGURO_USD, 2),
                                                                ROUND(@VAL_DOCUMENTO, 2)
                                                            )";

                                        using (SqlCommand comandoSQL = new SqlCommand(queryInsercao, conexaoSQL))
                                        {
                                            comandoSQL.Parameters.AddWithValue("@NR_DOC", NR_DOC);
                                            comandoSQL.Parameters.AddWithValue("@CD_DECLARACAO", CD_DECLARACAO);
                                            comandoSQL.Parameters.AddWithValue("@DT_DOCUMENTO", DT_DOCUMENTO);
                                            comandoSQL.Parameters.AddWithValue("@DT_ATRACACAO", DT_ATRACACAO);
                                            comandoSQL.Parameters.AddWithValue("@DT_REGISTRO", DT_REGISTRO);
                                            comandoSQL.Parameters.AddWithValue("@DT_DESEMBARACO", DT_DESEMBARACO);
                                            comandoSQL.Parameters.AddWithValue("@DT_RECEPCAO", DT_RECEPCAO);
                                            comandoSQL.Parameters.AddWithValue("@TP_DOCUMENTO", TP_DOCUMENTO);
                                            comandoSQL.Parameters.AddWithValue("@NUM_DOCUMENTO", NUM_DOCUMENTO);
                                            comandoSQL.Parameters.AddWithValue("@TIPO_DOC", TIPO_DOC);
                                            comandoSQL.Parameters.AddWithValue("@TP_MANIFESTO", TP_MANIFESTO);
                                            comandoSQL.Parameters.AddWithValue("@QTD_ADICOES", QTD_ADICOES);
                                            comandoSQL.Parameters.AddWithValue("@RECINTO", RECINTO);
                                            comandoSQL.Parameters.AddWithValue("@NR_IMPORTADOR", NR_IMPORTADOR);
                                            comandoSQL.Parameters.AddWithValue("@NOME_IMPORTADOR", NOME_IMPORTADOR);
                                            comandoSQL.Parameters.AddWithValue("@TP_IMPORTADOR", TP_IMPORTADOR);
                                            comandoSQL.Parameters.AddWithValue("@PAIS_ORIGEM", PAIS_ORIGEM);
                                            comandoSQL.Parameters.AddWithValue("@NOME_PAIS_ORIGEM", NOME_PAIS_ORIGEM);
                                            comandoSQL.Parameters.AddWithValue("@PB_CARGA", PB_CARGA);
                                            comandoSQL.Parameters.AddWithValue("@PL_CARGA", PL_CARGA);
                                            comandoSQL.Parameters.AddWithValue("@FRETE_USD", FRETE_USD);
                                            comandoSQL.Parameters.AddWithValue("@SEGURO_USD", SEGURO_USD);
                                            comandoSQL.Parameters.AddWithValue("@VAL_DOCUMENTO", VAL_DOCUMENTO);

                                            comandoSQL.ExecuteNonQuery();
                                            registros++;
                                            Console.ForegroundColor = ConsoleColor.Green;
                                            Console.WriteLine("DADO INSERIDO COM SUCESSO, NR_DOC NÚMERO: " + NR_DOC);
                                            





                                        }
                                    }
                                }
                                catch (Exception error)
                                {
                                    Console.BackgroundColor = ConsoleColor.Red;
                                    Console.WriteLine("Erro na inserção de dados" + error);
                                    Console.ReadKey();
                                }


                            }


                        }
                    }
                }




                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("FORAM INSERIDOS " + registros + " REGISTROS ");
                
            }
        }


    }
}
