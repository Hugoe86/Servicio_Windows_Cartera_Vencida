using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Windows.Forms;
using System.Data.SqlClient;
using SIAC.Constantes;

namespace Servicio_Cartera_Vencida_Simapag
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
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
        private void button1_Click(object sender, EventArgs e)
        {
            DataTable Dt_Consulta = new DataTable();
            DataTable Dt_Cuentas_Cartera_Vencida = new DataTable();
            DataTable Dt_Resultado = new DataTable();
            DataTable Dt_Existencia = new DataTable();
            DataRow Dr_ = null;
            String Str_Mes = "";
            String Str_Anio = "";
            Dictionary<Int32, String> Dic_Meses;
            StringBuilder Str_Cuentas_Con_Convenio = new StringBuilder();
            DataTable Dt_Consulta_Cuentas_Con_Convenio = new DataTable();

            try
            {
                Dt_Consulta_Cuentas_Con_Convenio = Consulta_Convenios();


                foreach (DataRow Registro in Dt_Consulta_Cuentas_Con_Convenio.Rows)
                {
                    Str_Cuentas_Con_Convenio.Append("'" + Registro["Predio_id"].ToString() + "',");
                }

                if (Str_Cuentas_Con_Convenio.Length > 0)
                {
                    Str_Cuentas_Con_Convenio.Remove(Str_Cuentas_Con_Convenio.Length - 1, 1);
                }



                Dic_Meses = Crear_Diccionario_Meses();

                Dt_Resultado = Crear_Tabla_Final();
                Dt_Consulta = Consulta_Cartera_Vencida(Str_Cuentas_Con_Convenio.ToString());

                foreach (DataRow Registro in Dt_Consulta.Rows)
                {
                    Str_Anio = Registro["Año"].ToString();
                    Str_Mes = Registro["Mes"].ToString();
                    break;
                }


                var var_giros = Dt_Consulta.AsEnumerable()
                                 .Select(row => new
                                 {
                                     Giro = row.Field<String>("nombre_giro")
                                     ,Giro_ID= row.Field<String>("giro_Id")
                                 }).Distinct();

                //      se genera la informacion de la cartera vencida
                foreach (var Fila_Giro in var_giros)
                {
                    //  se obtiene la informacion del tipo de giro
                    Dt_Cuentas_Cartera_Vencida = (from fila in Dt_Consulta.AsEnumerable()
                                                  where fila.Field<String>("giro_id") == (Fila_Giro.Giro_ID)

                                                  select fila
                                       ).AsDataView().ToTable();


                    double Db_Total = (from ord in Dt_Cuentas_Cartera_Vencida.AsEnumerable()
                                       select ord.Field<double>("Monto_Adeudo"))
                                            .Sum();


                    double Db_Cantidad = (from ord in Dt_Cuentas_Cartera_Vencida.AsEnumerable()
                                          select ord.Field<string>("nombre_giro"))
                                            .Count();

                    Dt_Existencia = Consultar_Si_Esta_Registrada(Fila_Giro.Giro_ID, Str_Anio);



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
                        }
                        else
                        {
                            //  update
                            Actualizar(Registro);
                        }
                    }
                }

                Grid_Resultado.DataSource = Dt_Resultado;
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
                        Mi_Sql.Append(Registro["Año"].ToString());
                        Mi_Sql.Append(", '" + Registro["giro"].ToString() + "'");
                        Mi_Sql.Append(", '" + Registro["giro_Id"].ToString() + "'");
                        Mi_Sql.Append(", " + Registro["monto"].ToString() + "");
                        Mi_Sql.Append(", " + Registro["cantidad"].ToString() + "");
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
                        Mi_Sql.Append(", " + Dic_Meses[Convert.ToInt32(Registro["Mes"].ToString())] + " = " + Registro["monto"].ToString());
                        Mi_Sql.Append(", " + Dic_Meses[Convert.ToInt32(Registro["Mes"].ToString())] + "_Cuentas" + "= " + Registro["cantidad"].ToString());

                        Mi_Sql.Append(" where ");
                        Mi_Sql.Append(" Anio = " + Registro["Año"].ToString());
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
                        //**********************************************************************************************
                        //**********************************************************************************************
                        Mi_Sql.Append("SELECT ");
                        Mi_Sql.Append("  COUNT(DISTINCT (f.Periodo_Facturacion)) AS Meses_Adeudo");
                        Mi_Sql.Append(", year(GETDATE()) AS Año");
                        Mi_Sql.Append(", MONTH(GETDATE()) AS Mes");
                        Mi_Sql.Append(", p.Predio_ID");
                        Mi_Sql.Append(", (t.Abreviatura) AS Tarifa");
                        Mi_Sql.Append(", cast(isnull(( " +
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
                        Mi_Sql.Append(", g.giro_id as Giro_Id");
                        Mi_Sql.Append(",g.Nombre_Giro + ' (' + g.clave + ')'  as Nombre_Giro");

                        //**********************************************************************************************
                        //**********************************************************************************************
                        //  From
                        //**********************************************************************************************
                        //**********************************************************************************************
                        Mi_Sql.Append(" FROM Cat_Cor_Predios p ");
                        Mi_Sql.Append(" JOIN Ope_Cor_Facturacion_Recibos f ON f.Predio_ID = p.Predio_ID ");
                        Mi_Sql.Append(" JOIN Ope_Cor_Facturacion_Recibos_Detalles fd ON fd.No_Factura_Recibo = f.No_Factura_Recibo ");
                        Mi_Sql.Append(" JOIN Cat_Cor_Usuarios u ON p.Usuario_ID = u.USUARIO_ID ");
                        Mi_Sql.Append(" JOIN Cat_Cor_Tarifas t ON t.Tarifa_ID = p.Tarifa_ID ");
                        Mi_Sql.Append(" JOIN Cat_Cor_Regiones r ON r.Region_ID = p.Region_ID ");
                        Mi_Sql.Append(" LEFT OUTER JOIN Cat_Cor_Colonias c ON c.COLONIA_ID = p.Colonia_ID ");
                        Mi_Sql.Append(" LEFT OUTER JOIN Cat_Cor_Calles ca ON ca.CALLE_ID = p.Calle_ID ");
                        Mi_Sql.Append(" left outer JOIN CAT_COR_TIPOS_CUOTAS cu ON cu.CUOTA_ID = t.Cuota_ID ");
                        Mi_Sql.Append(" JOIN Cat_Cor_Giros_Actividades ga ON ga.Actividad_Giro_ID = p.Giro_Actividad_ID ");
                        Mi_Sql.Append(" JOIN Cat_Cor_Giros g ON g.GIRO_ID = ga.Giro_ID ");

                        //**********************************************************************************************
                        //**********************************************************************************************
                        //  where
                        //**********************************************************************************************
                        //**********************************************************************************************
                        Mi_Sql.Append(" WHERE f.Estatus_Recibo IN (" +
                                        " 'PENDIENTE'" +
                                        " ,'PARCIAL'" +
                                        " )");

                        if (!String.IsNullOrEmpty(Predios_Convenio))
                        {
                            Mi_Sql.Append(" and p.predio_id not in (" + Predios_Convenio + ")");
                        }


                        //**********************************************************************************************
                        //**********************************************************************************************
                        //  group by
                        //**********************************************************************************************
                        //**********************************************************************************************
                        Mi_Sql.Append(" GROUP BY ");
                        Mi_Sql.Append("  p.Region_ID");
                        Mi_Sql.Append(", p.Predio_ID");
                        Mi_Sql.Append(", g.GIRO_ID");
                        Mi_Sql.Append(", t.Abreviatura");
                        Mi_Sql.Append(", g.clave");
                        Mi_Sql.Append(", g.Nombre_Giro");

                        //**********************************************************************************************
                        //**********************************************************************************************
                        //  having
                        //**********************************************************************************************
                        //**********************************************************************************************
                        Mi_Sql.Append(" having ");
                        Mi_Sql.Append(" COUNT(DISTINCT (f.No_Factura_Recibo)) >= (select pa.Meses_Adeudo_Para_Congelar from Cat_Cor_Parametros pa) ");//    se obtiene del parametro
                        Mi_Sql.Append("");
                        Mi_Sql.Append("");
                        
                        
                        //**********************************************************************************************
                        //**********************************************************************************************
                        
                        




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
