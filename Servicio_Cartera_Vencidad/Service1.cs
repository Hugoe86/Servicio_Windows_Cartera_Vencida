using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Timers;
using System.Data.SqlClient;
using SIAC.Constantes;
using System.IO;
using System.Globalization;

namespace Servicio_Cartera_Vencidad
{
    public partial class Service1 : ServiceBase
    {
        public Timer Tiempo;

        public Service1()
        {
            InitializeComponent();
            Tiempo = new Timer();
            Tiempo.Interval = 900000; // 30000 = 30 seg     // 600000 = 10 minutos // 900000 = 15 minutos
            Tiempo.Elapsed += new ElapsedEventHandler(Tiempo_Contador);
        }
        /////*******************************************************************************************************
        ///// <summary>
        ///// 
        ///// </summary>
        ///// <returns></returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>1</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        protected override void OnStart(string[] args)
        {
            Tiempo.Enabled = true;
        }
        /////*******************************************************************************************************
        ///// <summary>
        ///// 
        ///// </summary>
        ///// <returns></returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>1</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        protected override void OnStop()
        {
        }

        /////*******************************************************************************************************
        ///// <summary>
        ///// 
        ///// </summary>
        ///// <returns></returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>1</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        public void Tiempo_Contador(object Sender, EventArgs e)
        {
            DataTable Dt_Consulta = new DataTable();
            DataTable Dt_Cuentas_Cartera_Vencida = new DataTable();
            DataTable Dt_Resultado = new DataTable();
            DataTable Dt_Existencia = new DataTable();
            DataRow Dr_ = null;
            String Str_Mes = "";
            String Str_Anio = "";
            Dictionary<Int32, String> Dic_Meses;
            DateTime Dtime_Hora = DateTime.Now;
            //StreamWriter SW = new StreamWriter("C:\\Servicios_siac\\Historial.txt", true);
            StringBuilder Str_Cuentas_Con_Convenio = new StringBuilder();
            DataTable Dt_Consulta_Cuentas_Con_Convenio = new DataTable();


            try
            {
                //SW.WriteLine("************************************************************");

                if (Dtime_Hora.Hour >= 18 && Dtime_Hora.Hour <= 19)
                {
                    Dic_Meses = Crear_Diccionario_Meses();

                    Dt_Resultado = Crear_Tabla_Final();
                    
                    Dt_Consulta_Cuentas_Con_Convenio = Consulta_Convenios();


                    foreach (DataRow Registro in Dt_Consulta_Cuentas_Con_Convenio.Rows)
                    {
                        Str_Cuentas_Con_Convenio.Append("'" + Registro["Predio_id"].ToString() + "',");
                    }

                    if (Str_Cuentas_Con_Convenio.Length > 0)
                    {
                        Str_Cuentas_Con_Convenio.Remove(Str_Cuentas_Con_Convenio.Length - 1, 1);
                    }

                    Dt_Consulta = Consulta_Cartera_Vencida(Str_Cuentas_Con_Convenio.ToString());


                    foreach (DataRow Registro in Dt_Consulta.Rows)
                    {
                        Str_Anio = Registro["Año"].ToString();
                        Str_Mes = Registro["Mes"].ToString();
                        break;
                    }
                    //SW.WriteLine("se obtiene año y mes" + DateTime.Now.ToString());


                    var var_giros = Dt_Consulta.AsEnumerable()
                                     .Select(row => new
                                     {
                                         Giro = row.Field<String>("nombre_giro"),
                                         Giro_ID = row.Field<String>("giro_Id")
                                     }).Distinct();

                    //SW.WriteLine("se obtiene los giros" + DateTime.Now.ToString());

                    //      se genera la informacion de la cartera vencida
                    foreach (var Fila_Giro in var_giros)
                    {
                        //  se obtiene la informacion del tipo de giro
                        Dt_Cuentas_Cartera_Vencida = (from fila in Dt_Consulta.AsEnumerable()
                                                      where fila.Field<String>("giro_id") == (Fila_Giro.Giro_ID)
                                                      select fila
                                           ).AsDataView().ToTable();


                        //SW.WriteLine("se convierte a tabla" + DateTime.Now.ToString());

                        double Db_Total = (from ord in Dt_Cuentas_Cartera_Vencida.AsEnumerable()
                                           select ord.Field<double>("Monto_Adeudo"))
                                                .Sum();
                        //SW.WriteLine("se obtiene el monto de adeudo" + DateTime.Now.ToString());

                        double Db_Cantidad = (from ord in Dt_Cuentas_Cartera_Vencida.AsEnumerable()
                                              select ord.Field<string>("nombre_giro"))
                                                .Count();

                        //SW.WriteLine("se obtiene el count" + DateTime.Now.ToString());

                        Dt_Existencia = Consultar_Si_Esta_Registrada(Fila_Giro.Giro_ID, Str_Anio);
                        //SW.WriteLine("validacion" + DateTime.Now.ToString());


                        Dr_ = Dt_Resultado.NewRow();

                        Dr_["Giro"] = Fila_Giro.Giro;
                        Dr_["Giro_ID"] = Fila_Giro.Giro_ID;
                        Dr_["Mes"] = Str_Mes;
                        Dr_["Año"] = Str_Anio;
                        Dr_["Monto"] = Db_Total;
                        Dr_["Cantidad"] = Db_Cantidad;

                        if (Dt_Existencia.Rows.Count > 0)
                        {
                            Dr_["Registrado"] = "SI";
                        }
                        else
                        {
                            Dr_["Registrado"] = "NO";
                        }

                        Dt_Resultado.Rows.Add(Dr_);
                        Dt_Resultado.AcceptChanges();

                    }

                    //  se realizaran las operaciones con la cual se guardara la informacion
                    foreach (DataRow Registro in Dt_Resultado.Rows)
                    {
                        if (!String.IsNullOrEmpty(Registro["Registrado"].ToString()))
                        {
                            if (Registro["Registrado"].ToString() == "NO")
                            {
                                //  insert
                                Insertar(Registro);
                                //SW.WriteLine("insercion" + DateTime.Now.ToString());
                            }
                            else
                            {
                                //  update
                                Actualizar(Registro);
                                //SW.WriteLine("actualizacion" + DateTime.Now.ToString());
                            }
                        }
                    }

                }

                //SW.WriteLine("************************************************************");

            }
            catch (Exception Ex)
            {
                //SW.WriteLine("Error: " + Ex.Message);
                throw new Exception("Error: " + Ex.Message);
            }
            finally
            {
                //SW.Close();
            }

        }



        /////*******************************************************************************************************
        ///// <summary>
        ///// genera un datatable nuevo con los campos para la 
        ///// </summary>
        ///// <returns>un datatable con los campos para mostrar accesos e ingresos por año y mes</returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>13-Enero-2016</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        public static Dictionary<Int32, String> Crear_Diccionario_Meses()
        {
            var Diccionario = new Dictionary<Int32, String>();

            try
            {
                Diccionario.Add(1, "Enero");
                Diccionario.Add(2, "Febrero");
                Diccionario.Add(3, "Marzo");
                Diccionario.Add(4, "Abril");
                Diccionario.Add(5, "Mayo");
                Diccionario.Add(6, "Junio");
                Diccionario.Add(7, "Julio");
                Diccionario.Add(8, "Agosto");
                Diccionario.Add(9, "Septiembre");
                Diccionario.Add(10, "Octubre");
                Diccionario.Add(11, "Noviembre");
                Diccionario.Add(12, "Diciembre");
            }
            catch (Exception Ex)
            {
                throw new Exception("Error: " + Ex.Message);
            }

            return Diccionario;
        }

        /////*******************************************************************************************************
        ///// <summary>
        ///// genera un datatable nuevo con los campos para la 
        ///// </summary>
        ///// <returns>un datatable con los campos para mostrar accesos e ingresos por año y mes</returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>13-Enero-2016</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        private DataTable Crear_Tabla_Final()
        {
            DataTable Dt_Consulta = new DataTable();

            try
            {
                Dt_Consulta.Columns.Add("Giro");
                Dt_Consulta.Columns.Add("Giro_ID");
                Dt_Consulta.Columns.Add("Monto");
                Dt_Consulta.Columns.Add("Mes");
                Dt_Consulta.Columns.Add("Año");
                Dt_Consulta.Columns.Add("Cantidad");
                Dt_Consulta.Columns.Add("Registrado");

            }
            catch (Exception Ex)
            {
                throw new Exception("Error: " + Ex.Message);
            }

            return Dt_Consulta;
        }


        /////*******************************************************************************************************
        ///// <summary>
        ///// Crear el registro de la cartera vencida
        ///// </summary>
        ///// <returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>13-Enero-2016</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        private void Insertar(DataRow Registro)
        {
            StringBuilder Mi_Sql = new StringBuilder();
            Dictionary<Int32, String> Dic_Meses;

            try
            {
                Dic_Meses = Crear_Diccionario_Meses();

                using (SqlConnection conexion = new SqlConnection(Cls_Constantes.Str_Conexion))
                {

                    conexion.Open();

                    using (SqlCommand comando = conexion.CreateCommand())
                    {

                        Mi_Sql.Append("Insert into Ope_Cor_Cc_Cartera_Vencidad_Historico ");
                        Mi_Sql.Append("(");
                        Mi_Sql.Append("Anio");
                        Mi_Sql.Append(", Tipo");
                        Mi_Sql.Append(", giro_Id");
                        Mi_Sql.Append(", " + Dic_Meses[Convert.ToInt32(Registro["Mes"].ToString())]);
                        Mi_Sql.Append(", " + Dic_Meses[Convert.ToInt32(Registro["Mes"].ToString())] + "_Cuentas");
                        Mi_Sql.Append(", fecha");
                        Mi_Sql.Append(")");

                        Mi_Sql.Append(" Values ");

                        Mi_Sql.Append("(");
                        Mi_Sql.Append(Convert.ToDouble(Registro["Año"].ToString()).ToString(new CultureInfo("es-MX")));
                        Mi_Sql.Append(", '" + Registro["giro"].ToString() + "'");
                        Mi_Sql.Append(", '" + Registro["giro_Id"].ToString() + "'");
                        Mi_Sql.Append(", " + Convert.ToDouble(Registro["monto"].ToString()).ToString(new CultureInfo("es-MX")) + "");
                        Mi_Sql.Append(", " + Convert.ToDouble(Registro["cantidad"].ToString()).ToString(new CultureInfo("es-MX")) + "");
                        Mi_Sql.Append(", getdate()");
                        Mi_Sql.Append(")");

                        comando.CommandText = Mi_Sql.ToString();
                        comando.CommandTimeout = 60;
                        comando.ExecuteNonQuery();

                    }
                }

            }
            catch (Exception Ex)
            {
                throw new Exception("Error: " + Ex.Message);
            }
        }


        /////*******************************************************************************************************
        ///// <summary>
        ///// Crear el registro de la cartera vencida
        ///// </summary>
        ///// <returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>13-Enero-2016</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        private void Actualizar(DataRow Registro)
        {
            StringBuilder Mi_Sql = new StringBuilder();
            Dictionary<Int32, String> Dic_Meses;

            try
            {
                Dic_Meses = Crear_Diccionario_Meses();

                using (SqlConnection conexion = new SqlConnection(Cls_Constantes.Str_Conexion))
                {

                    conexion.Open();

                    using (SqlCommand comando = conexion.CreateCommand())
                    {

                        Mi_Sql.Append("update Ope_Cor_Cc_Cartera_Vencidad_Historico set");
                        Mi_Sql.Append(" fecha = getdate() ");
                        Mi_Sql.Append(", " + Dic_Meses[Convert.ToInt32(Registro["Mes"].ToString())] + " = " + Convert.ToDouble(Registro["monto"].ToString()).ToString(new CultureInfo("es-MX")));
                        Mi_Sql.Append(", " + Dic_Meses[Convert.ToInt32(Registro["Mes"].ToString())] + "_Cuentas" + "= " + Convert.ToDouble(Registro["cantidad"].ToString()).ToString(new CultureInfo("es-MX")));

                        Mi_Sql.Append(" where ");
                        Mi_Sql.Append(" Anio = " + Convert.ToDouble(Registro["Año"].ToString()).ToString(new CultureInfo("es-MX")));
                        Mi_Sql.Append(" and giro_id = '" + Registro["giro_id"].ToString() + "'");

                        comando.CommandText = Mi_Sql.ToString();
                        comando.CommandTimeout = 60;
                        comando.ExecuteNonQuery();

                    }
                }

            }
            catch (Exception Ex)
            {
                throw new Exception("Error: " + Ex.Message);
            }
        }


        /////*******************************************************************************************************
        ///// <summary>
        ///// genera un datatable nuevo con los campos para la 
        ///// </summary>
        ///// <returns>un datatable con los campos para mostrar accesos e ingresos por año y mes</returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>13-Enero-2016</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        private DataTable Consulta_Convenios()
        {
            DataTable Dt_Consulta = new DataTable();
            StringBuilder Mi_Sql = new StringBuilder();
            DataSet ds;
            SqlDataAdapter da;


            try
            {

                using (SqlConnection conexion = new SqlConnection(Cls_Constantes.Str_Conexion))
                {

                    conexion.Open();

                    using (SqlCommand comando = conexion.CreateCommand())
                    {
                        Mi_Sql.Append("SELECT Predio_ID from Ope_Cor_Convenios where Estatus = 'PENDIENTE' ");


                        comando.CommandText = Mi_Sql.ToString();
                        comando.CommandTimeout = 100;
                        da = new SqlDataAdapter(comando);
                        ds = new DataSet();
                        da.Fill(ds);

                        Dt_Consulta = ds.Tables[0];
                    }
                }

            }
            catch (Exception Ex)
            {
                throw new Exception("Error: " + Ex.Message);
            }
            return Dt_Consulta;

        }



        /////*******************************************************************************************************
        ///// <summary>
        ///// genera un datatable nuevo con los campos para la 
        ///// </summary>
        ///// <returns>un datatable con los campos para mostrar accesos e ingresos por año y mes</returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>13-Enero-2016</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        private DataTable Consulta_Cartera_Vencida(String Predios_Convenio)
        {
            DataTable Dt_Consulta = new DataTable();
            StringBuilder Mi_Sql = new StringBuilder();
            DataSet ds;
            SqlDataAdapter da;


            try
            {

                using (SqlConnection conexion = new SqlConnection(Cls_Constantes.Str_Conexion))
                {

                    conexion.Open();

                    using (SqlCommand comando = conexion.CreateCommand())
                    {

                        Mi_Sql.Append("Select ");

                        Mi_Sql.Append(" cast(isnull(( " +
                                                     " SELECT sum(frd.Total_Saldo)" +
                                                     " FROM Ope_Cor_Facturacion_Recibos fr" +
                                                     " JOIN Ope_Cor_Facturacion_Recibos_Detalles frd ON fr.No_Factura_Recibo = frd.No_Factura_Recibo" +
                                                     " WHERE fr.Estatus_Recibo IN (" +
                                                             " 'PENDIENTE'" +
                                                             " ,'PARCIAL'" +
                                                             ")" +
                                                         " AND fr.Predio_ID = p.Predio_ID" +
                                                         " and (" +
                                                                 " frd.Concepto_ID = (select p.CONCEPTO_AGUA from Cat_Cor_Parametros p)" +
                                                                 " or frd.Concepto_ID = (select p.Concepto_Agua_Comercial from Cat_Cor_Parametros p)" +
                                                                 " or frd.Concepto_ID = (select p.CONCEPTO_DRENAJE from Cat_Cor_Parametros p)" +
                                                                 " or frd.Concepto_ID = (select p.CONCEPTO_SANAMIENTO from Cat_Cor_Parametros p)" +
                                                         " )" +
                                                     "), 0) AS DOUBLE PRECISION) AS Monto_Adeudo");
                        

                        Mi_Sql.Append(",g.Nombre_Giro + ' (' + g.clave + ')'  as Nombre_Giro" +
                                    ",MONTH(getdate()) AS Mes" +
                                    ",year(getdate()) AS Año" +
                                    ",g.giro_id AS Giro_Id" +

                                " FROM Cat_Cor_Predios p" +
                                " JOIN Cat_Cor_Usuarios u ON p.usuario_id = u.USUARIO_ID" +
                                " JOIN Cat_Cor_Tarifas t ON t.Tarifa_ID = p.Tarifa_ID" +
                                " JOIN Ope_Cor_Facturacion_Recibos f ON f.Predio_ID = p.Predio_ID" +
                                " JOIN Ope_Cor_Facturacion_Recibos_Detalles fd ON fd.No_Factura_Recibo = f.No_Factura_Recibo" +
                                " JOIN Cat_Cor_Regiones r ON r.Region_ID = p.Region_ID" +
                                " JOIN CAT_COR_TIPOS_CUOTAS cu ON cu.CUOTA_ID = t.Cuota_ID" +
                                " JOIN Cat_Cor_Giros_Actividades ga ON ga.Actividad_Giro_ID = p.Giro_Actividad_ID" +
                                " JOIN Cat_Cor_Giros g ON g.GIRO_ID = ga.Giro_ID" +

                                " WHERE f.Estatus_Recibo IN (" +
                                        " 'PENDIENTE'" +
                                        " ,'PARCIAL'" +
                                        " )" );

                                    //" AND p.cortado = 'NO'" +
                        
                        if (!String.IsNullOrEmpty(Predios_Convenio))
                        {
                            Mi_Sql.Append(" and p.predio_id not in (" + Predios_Convenio + ")");
                        }




                        Mi_Sql.Append(" GROUP by " +
                                    "   g.Nombre_Giro" +
                                    " , p.Predio_ID" +
                                    " , g.giro_id " +
                                    " , g.clave " +

                                " HAVING COUNT(DISTINCT (f.No_Factura_Recibo)) >= 6" +
                                    " AND SUM(fd.Total_Saldo) BETWEEN 1" +
                                        " AND 99999999");


                        comando.CommandText = Mi_Sql.ToString();
                        comando.CommandTimeout = 100;
                        da = new SqlDataAdapter(comando);
                        ds = new DataSet();
                        da.Fill(ds);

                        Dt_Consulta = ds.Tables[0];

                    }
                }


            }
            catch (Exception Ex)
            {
                throw new Exception("Error: " + Ex.Message);
            }
            return Dt_Consulta;

        }




        /////*******************************************************************************************************
        ///// <summary>
        ///// genera un datatable nuevo con los campos para la 
        ///// </summary>
        ///// <returns>un datatable con los campos para mostrar accesos e ingresos por año y mes</returns>
        ///// <creo>Hugo Enrique Ramírez Aguilera</creo>
        ///// <fecha_creo>13-Enero-2016</fecha_creo>
        ///// <modifico></modifico>
        ///// <fecha_modifico></fecha_modifico>
        ///// <causa_modificacion></causa_modificacion>
        ///*******************************************************************************************************
        private DataTable Consultar_Si_Esta_Registrada(String Tipo, String Año)
        {
            DataTable Dt_Consulta = new DataTable();
            StringBuilder Mi_Sql = new StringBuilder();
            DataSet ds;
            SqlDataAdapter da;


            try
            {

                using (SqlConnection conexion = new SqlConnection(Cls_Constantes.Str_Conexion))
                {

                    conexion.Open();

                    using (SqlCommand comando = conexion.CreateCommand())
                    {

                        Mi_Sql.Append("select * from Ope_Cor_Cc_Cartera_Vencidad_Historico" +
                                        " where Anio = " + Año +
                                        " and giro_Id = '" + Tipo + "' ");


                        comando.CommandText = Mi_Sql.ToString();
                        comando.CommandTimeout = 100;
                        da = new SqlDataAdapter(comando);
                        ds = new DataSet();
                        da.Fill(ds);

                        Dt_Consulta = ds.Tables[0];

                    }
                }


            }
            catch (Exception Ex)
            {
                throw new Exception("Error: " + Ex.Message);
            }
            return Dt_Consulta;

        }

    }
}
