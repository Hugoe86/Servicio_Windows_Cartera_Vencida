using System;
using System.Data;
using System.Configuration;
using System.Linq;

using System.Xml.Linq;
/// <summary>
/// Summary description for Cls_Constantes
/// </summary>
/// 
namespace SIAC.Constantes
{
    public class Cls_Constantes
    {
        public static string Str_Conexion = "server = 200.33.34.9;Database = SIMAPAG_20161015; Uid = usrsimapag; Pwd = C0nt3l16";
        //Server=10.10.1.6; Database=SIAC_OPDAPAS_ADMINISTRATIVO; Uid=usropdapas; Pwd=opdapas2013;"

        public static string nombre_pagina_maestra = "ctl00$Cph_Area_Trabajo1$";
        
    }
  
    ///**********************************************************************************************************************************
    ///                                                           ATENCION CIUDADANA
    ///**********************************************************************************************************************************

    #region Atencion Ciudadana

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Ate_Peticiones
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_ATE_PETICIONES
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Ate_Peticiones
    {
        public const String Tabla_Ope_Ate_Peticiones = "OPE_ATE_PETICIONES";
        public const String Campo_Peticion_ID = "PETICION_ID";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Fecha_Peticion = "FECHA_PETICION";
        public const String Campo_Fecha_Solucion_Probable = "FECHA_SOLUCION_PROBABLE";
        public const String Campo_Nivel_Importancia = "NIVEL_IMPORTANCIA";
        public const String Campo_Genera_Noticia = "GENERA_NOTICIA";
        public const String Campo_Nombre_Solicitante = "NOMBRE";
        public const String Campo_Apellido_Paterno = "APELLIDO_PATERNO";
        public const String Campo_Apellido_Materno = "APELLIDO_MATERNO";
        public const String Campo_Edad = "EDAD";
        public const String Campo_Sexo = "SEXO";
        public const String Campo_Calle_No = "CALLE_NO";//
        public const String Campo_Colonia_ID = "COLONIA_ID";
        public const String Campo_Localidad_ID = "LOCALIDAD_ID";
        public const String Campo_Codigo_Postal = "CODIGO_POSTAL";
        public const String Campo_Telefono = "TELEFONO";
        public const String Campo_Email = "E_MAIL";
        public const String Campo_Descripcion_Peticion = "DESCRIPCION_PETICION";
        public const String Campo_Asunto_ID = "ASUNTO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Area_ID = "AREA_ID";
        public const String Campo_Origen_De_Registro = "ORIGEN_DE_REGISTRO";
        public const String Campo_Descripcion_Solucion = "DESCRIPCION_SOLUCION";
        public const String Campo_Fecha_Solucion_Real = "FECHA_SOLUCION_REAL";
        public const String Campo_Asignado = "ASIGNADO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Ope_Ate_Seguimiento_Peticiones
    ///DESCRIPCIÓN: Segimiento peticiones
    ///PARAMETROS: 
    ///CREO: jesus toledo
    ///FECHA_CREO: 03/31/2011 01:37:25 p.m.
    ///MODIFICO: 
    ///FECHA_MODIFICO:
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Ate_Seguimiento_Peticiones
    {
        public const String Tabla_Ope_Ate_Seguimiento_Peticiones = "OPE_ATE_SEGUIMIENTO_PETICIONES";
        public const String Campo_Seguimiento_ID = "SEGUIMIENTO_ID";
        public const String Campo_Peticion_ID = "PETICION_ID";
        public const String Campo_Asunto_ID = "ASUNTO_ID";
        public const String Campo_Area_ID = "AREA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Observaciones = "OBSERVACIONES";
        public const String Campo_Fecha_Asignacion = "FECHA_ASIGNACION";
    }
    
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Ate_Asuntos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_ATE_ASUNTOS
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 19/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Ate_Asuntos
    {
        public const String Tabla_Cat_Ate_Asuntos = "CAT_ATE_ASUNTOS";
        public const String Campo_AsuntoID = "ASUNTO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_DependenciaID = "DEPENDENCIA_ID";
        public const String Campo_AreaID = "AREA_ID";
        public const String Campo_UsuarioCreo = "USUARIO_CREO";
        public const String Campo_FechaCreo = "FECHA_CREO";
        public const String Campo_UsuarioModifico = "USUARIO_MODIFICO";
        public const String Campo_FechaModifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
  NOMBRE DE LA CLASE:Ope_Ate_Asigna_Pet_Empleado
  DESCRIPCIÓN: Clase que contiene los datos de la Tabla Ope_Ate_Asigna_Pet_Empleado
  PARÁMETROS :
  CREO       : Silvia Morales Portuhondo
  FECHA_CREO : 30/Septiembre/2010
  MODIFICO   :
  FECHA_MODIFICO:
  CAUSA_MODIFICACIÓN:
 *******************************************************************************/
    public class Ope_Ate_Asigna_Pet_Empleado
    {
        public const String Tabla_Ope_Asigna_Pet_Empleado = "OPE_ATE_ASIGNA_PET_EMPLEADO";
        public const String Campo_Asignacion_ID = "ASIGNACION_ID";
        public const String Campo_Peticion_ID = "PETICION_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Vigente = "VIGENTE";
        public const String Campo_UsuarioCreo = "USUARIO_CREO";
        public const String Campo_FechaCreo = "FECHA_CREO";
        public const String Campo_UsuarioModifico = "USUARIO_MODIFICO";
        public const String Campo_FechaModifico = "FECHA_MODIFICO";

    }//fin de Ope_Ate_Asigna_Pet_Empleado

   

   
    ///*******************************************************************************
    ///NOMBRE DE LA CLASE: Detalle_Ate_Peticiones
    ///       DESCRIPCIÓN: Clase que contiene los campos de la tabla Detalle_Ate_Peticiones
    ///              CREO: Alberto Pantoja Hernández
    ///        FECHA_CREO: 26/8/2010
    ///          MODIFICO: 
    ///    FECHA_MODIFICO: 
    ///CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************
    public class Detalle_Ate_Peticiones
    {
        public const String Tabla_Detalle_Ate_Peticiones = "DETALLE_ATE_PETICIONES";
        public const String Campo_Detalle_Peticion_ID = "DETALLE_PETICION_ID";
        public const String Campo_Peticion_ID = "PETICION_ID";
        public const String Campo_Dependencia_ID_Origen = "DEPENDENCIA_ID_ORIGEN";
        public const String Campo_Dependencia_ID_Destino = "DEPENDENCIA_ID_DESTINO";
        public const String Campo_Descripcion_Cambio = "DESCRIPCION_CAMBIO";
        public const String Campo_Fecha_Asignacion_Cambio = "FECHA_ASIGNACION_CAMBIO";
        public const String Campo_Area_ID = "AREA_ID";
        public const String Campo_UsuarioCreo = "USUARIO_CREO";
        public const String Campo_FechaCreo = "FECHA_CREO";
        public const String Campo_UsuarioModifico = "USUARIO_MODIFICO";
        public const String Campo_FechaModifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Ate_Colonias
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_ATE_COLONIAS
    /// PARÁMETROS :
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Ate_Colonias
    {
        public const String Tabla_Cat_Ate_Colonias = "CAT_ATE_COLONIAS";
        public const String Campo_Colonia_ID = "COLONIA_ID";
        public const String Campo_Tipo_Colonia_ID = "TIPO_COLONIA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Clave = "Clave";
		public const String Campo_Costo_Construccion = "COSTO_CONSTRUCCION";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    #endregion

    ///**********************************************************************************************************************************
    ///                                                                TRAMITES
    ///**********************************************************************************************************************************
    
    #region Tramites
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Tra_Subprocesos_Perfiles
    ///DESCRIPCIÓN: Clase que contiene los campos de la tabla TRA_SUBPROCESOS_PERFILES
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 06/Octubre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Tra_Subprocesos_Perfiles
    {
        public const String Tabla_Tra_Subprocesos_Perfiles = "TRA_SUBPROCESOS_PERFILES";
        public const String Campo_Subproceso_ID = "SUBPROCESO_ID";
        public const String Campo_Perfil_ID = "PERFIL_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    
    
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Tra_Tramites
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_TRA_TRAMITES
    /// PARÁMETROS :
    /// CREO       : JESUS TOLEDO
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Tra_Tramites
    {
        public const String Tabla_Cat_Tra_Tramites = "CAT_TRA_TRAMITES";
        public const String Campo_Tramite_ID = "TRAMITE_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Cuenta_ID = "NUMERO_CUENTA";
        public const String Campo_Clave_Tramite = "CLAVE_TRAMITE";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Tiempo_Estimado = "TIEMPO_ESTIMADO";
        public const String Campo_Costo = "COSTO";
        public const String Campo_Solicitar_Internet = "SOLICITAR_INTERNET";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Tra_Datos_Tramite
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_TRA_DATOS_TRAMITE
    /// PARÁMETROS :
    /// CREO       : JESUS TOLEDO
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Tra_Datos_Tramite
    {
        public const String Tabla_Cat_Tra_Datos_Tramite = "CAT_TRA_DATOS_TRAMITE";
        public const String Campo_Dato_ID = "DATO_ID";
        public const String Campo_Tramite_ID = "TRAMITE_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Dato_Requerido = "DATO_REQUERIDO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Tra_Subprocesos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_TRA_SUBPROCESOS
    /// PARÁMETROS :
    /// CREO       :JESUS TOLEDO
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Tra_Subprocesos
    {
        public const String Tabla_Cat_Tra_Subprocesos = "CAT_TRA_SUBPROCESOS";
        public const String Campo_Subproceso_ID = "SUBPROCESO_ID";
        public const String Campo_Tramite_ID = "TRAMITE_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Valor = "VALOR";
        public const String Campo_Orden = "ORDEN";
        public const String Campo_Plantilla = "PLANTILLA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Tra_Plantillas
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_TRA_PLANTILLAS
    /// PARÁMETROS :
    /// CREO       :Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 22/Octubre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Tra_Plantillas
    {
        public const String Tabla_Cat_Tra_Plantillas = "CAT_TRA_PLANTILLAS";
        public const String Campo_Plantilla_ID = "PLANTILLA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Archivo = "ARCHIVO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Tra_Datos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_TRA_DATOS
    /// PARÁMETROS :
    /// CREO       : JESUS TOLEDO
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Tra_Datos
    {
        public const String Tabla_Ope_Tra_Datos = "OPE_TRA_DATOS";
        public const String Campo_Ope_Dato_ID = "OPE_DATO_ID";
        public const String Campo_Dato_ID = "DATO_ID";
        public const String Campo_Tramite_ID = "TRAMITE_ID";
        public const String Campo_Solicitud_ID = "SOLICITUD_ID";
        public const String Campo_Valor = "VALOR";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Tra_Solicitud
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_TRA_SOLICITUD
    /// PARÁMETROS :
    /// CREO       :JESUS TOLEDO
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Tra_Solicitud
    {
        public const String Tabla_Ope_Tra_Solicitud = "OPE_TRA_SOLICITUD";
        public const String Campo_Solicitud_ID = "SOLICITUD_ID";
        public const String Campo_Tramite_ID = "TRAMITE_ID";
        public const String Campo_Subproceso_ID = "SUBPROCESO_ID";
        public const String Campo_Clave_Solicitud = "CLAVE_SOLICITUD";
        public const String Campo_Porcentaje_Avance = "PORCENTAJE_AVANCE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Nombre_Solicitante = "NOMBRE";
        public const String Campo_Apellido_Paterno = "APELLIDO_PATERNO";
        public const String Campo_Apellido_Materno = "APELLIDO_MATERNO";
        public const String Campo_Correo_Electronico = "CORREO_ELECTRONICO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Fecha_Entrega = "FECHA_ENTREGA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Tra_Detalle_Documentos 
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla TRA_DETALLE_DOCUMENTOS
    /// PARÁMETROS :
    /// CREO       : Alberto Pantoja Hernández 
    /// FECHA_CREO : 20/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Tra_Detalle_Documentos
    {
        public const String Tabla_Tra_Detalle_Documentos = "TRA_DETALLE_DOCUMENTOS";
        public const String Campo_Detalle_Documento_ID = "DETALLE_DOCUMENTO_ID";
        public const String Campo_Documento_ID = "DOCUMENTO_ID";
        public const String Campo_Tramite_ID = "TRAMITE_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Tra_Detalle_Autorizaciones
    /// ESCRIPCIÓN : Clase que contiene los campos de la tabla TRA_DETALLE_AUTORIZACIONES
    /// PARÁMETROS :
    /// CREO       : Alberto Pantoja Hernández 
    /// FECHA_CREO : 20/8/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Tra_Detalle_Autorizaciones
    {
        public const String Tabla_Tra_Detalle_Autorizaciones = "TRA_DETALLE_AUTORIZACIONES";
        public const String Campo_Detalle_Autorizacion_ID = "DETALLE_AUTORIZACION_ID";
        public const String Campo_Perfil_ID = "PERFIL_ID";
        public const String Campo_Tramite_ID = "TRAMITE_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Tra_Puestos_Perfiles
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_TRA_PUESTOS_PERFILES
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Tra_Puestos_Perfiles
    {
        public const String Tabla_Cat_Tra_Puestos_Perfiles = "CAT_TRA_PUESTOS_PERFILES";
        public const String Campo_Perfil_ID = "PERFIL_ID";
        public const String Campo_Puesto_ID = "PUESTO_ID";
        public const String Campo_UsuarioCreo = "USUARIO_CREO";
        public const String Campo_FechaCreo = "FECHA_CREO";
        public const String Campo_UsuarioModifico = "USUARIO_MODIFICO";
        public const String Campo_FechaModifico = "FECHA_MODIFICO";
    }

 

    
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Tra_Documentos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_TRA_DOCUMENTOS
    /// PARÁMETROS :
    /// CREO       : Alberto Pantoja Hernández 
    /// FECHA_CREO : 20/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Tra_Documentos
    {
        public const String Tabla_Cat_Tra_Documentos = "CAT_TRA_DOCUMENTOS";
        public const String Campo_Documento_ID = "DOCUMENTO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Tra_Perfiles
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_TRA_PERFILES
    /// PARÁMETROS :
    /// CREO       : Alberto Pantoja Hernández 
    /// FECHA_CREO : 20/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Tra_Perfiles
    {
        public const String Tabla_Cat_Tra_Perfiles = "CAT_TRA_PERFILES";
        public const String Campo_Perfil_ID = "PERFIL_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Autoriza = "AUTORIZA";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Tra_Subproceso
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_TRA_SUBPROCESO
    /// PARÁMETROS :
    /// CREO       : Alberto Pantoja Hernández 
    /// FECHA_CREO : 20/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Tra_Subproceso
    {
        public const String Tabla_Ope_Tra_Subproceso = "OPE_TRA_SUBPROCESO";
        public const String Campo_Ope_Subproceso_ID = "OPE_SUBPROCESO_ID";
        public const String Campo_Subproceso_ID = "SUBPROCESO_ID";
        public const String Campo_Solicitud_ID = "SOLICITUD_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Fecha_Termino = "FECHA_TERMINO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Tra_Documentos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_TRA_DOCUMENTOS
    /// PARÁMETROS :
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Tra_Documentos
    {
        public const String Tabla_Ope_Tra_Documentos = "OPE_TRA_DOCUMENTOS";
        public const String Campo_Ope_Documento_ID = "OPE_DOCUMENTO_ID";
        public const String Campo_Solicitud_ID = "SOLICITUD_ID";
        public const String Campo_Detalle_Documento_ID = "DETALLE_DOCUMENTO_ID";
        public const String Campo_URL = "URL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    #endregion
       

    ///**********************************************************************************************************************************
    ///                                                                GENERALES
    ///**********************************************************************************************************************************
    #region Generales
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Grupos_Dependencias
    /// DESCRIPCIÓN: Clase que contiene los datos de la tabla CAT_GRUPOS_DEPENDENCIAS
    /// PARÁMETROS :
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO : 01/Junio/2011
    /// MODIFICO   :
    /// FECHA_MODIFICO:
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Grupos_Dependencias
    {
        public const String Tabla_Cat_Grupos_Dependencias = "CAT_GRUPOS_DEPENDENCIAS";
        public const String Campo_Grupo_Dependencia_ID = "GRUPO_DEPENDENCIA_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Estatus = "ESTATUS";

    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Apl_Verifica_Ingreso
    /// DESCRIPCIÓN: Clase que contiene los datos de la tabla Apl_Bitacora
    /// PARÁMETROS :
    /// CREO       : Gustavo Angeles Cruz
    /// FECHA_CREO : 20/Sep/2010
    /// MODIFICO   :
    /// FECHA_MODIFICO:
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Apl_Verifica_Ingreso
    {
        public const String Tabla_Apl_Verifica_Ingreso = "APL_VERIFICA_INGRESO";
        public const String Campo_Verifica_ID = "VERIFICA_ID";
        public const String Campo_Fecha_Primer_Ingreso = "FECHA";
    }//fin de Apl_Verifica_Ingreso

    /*******************************************************************************
  NOMBRE DE LA CLASE: Apl_Parametros
  DESCRIPCIÓN: Clase que contiene los datos de la Tabla Apl_Parametros
  PARÁMETROS :
  CREO       : Silvia Morales Portuhondo
  FECHA_CREO : 30/Septiembre/2010
  MODIFICO   :Susana Trigueros Armenta
  FECHA_MODIFICO:11/Octubre/2010
  CAUSA_MODIFICACIÓN: Estaba mal escritos algunos campos y se borraron 3 campos que ya no se ocuparan
     los cuales son: 
     * public const String Campo_Administrador_Sistema = "ADMINISTRADOR_SISTEMA";
     * public const String Campo_Administrador_Modulo = "ADMINISTRADOR_MODULO";
     * public const String Campo_Jefe_Dependencia = "JEFE_DEPENDENCIA";
     * public const String Campo_Jefe_Area = "JEFE_AREA";
     * public const String Campo_Empleado = "EMPLEADO";
 *******************************************************************************/
    public class Apl_Parametros
    {
        public const String Tabla_Apl_Parametros = "APL_PARAMETROS";
        public const String Campo_Correo_Saliente = "CORREO_SALIENTE";
        public const String Campo_Servidor_Correo = "SERVIDOR_CORREO";
        public const String Campo_Usuario_Correo = "USUARIO_CORREO";
        public const String Campo_Password_Correo = "PASSWORD_CORREO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }//fin de Apl_Parametros
    ///******************************************************************************
    /// NOMBRE DE LA CLASE: Apl_Bitacora
    /// DESCRIPCIÓN: Clase que contiene los datos de la tabla Apl_Bitacora
    /// PARÁMETROS :
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO : 27/Agosto/2010
    /// MODIFICO   :
    /// FECHA_MODIFICO:
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Apl_Bitacora
    {
        public const String Tabla_Apl_Bitacora = "APL_BITACORA";
        public const String Campo_Bitacora_ID = "BITACORA_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Fecha_Hora = "FECHA_HORA";
        public const String Campo_Accion = "ACCION";
        public const String Campo_Recurso = "RECURSO";
        public const String Campo_Recurso_ID = "RECURSO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";

    }//fin de Apl_Bitacora 


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Ate_Localidades
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla Cat_Ate_Localidades
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 23/Septiembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Ate_Localidades
    {
        public const String Tabla_Cat_Ate_Localidades = "CAT_ATE_LOCALIDADES";
        public const String Campo_LocalidadID = "LOCALIDAD_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_UsuarioCreo = "USUARIO_CREO";
        public const String Campo_FechaCreo = "FECHA_CREO";
        public const String Campo_UsuarioModifico = "USUARIO_MODIFICO";
        public const String Campo_FechaModifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Bitacora
    /// DESCRIPCIÓN: Clase que contiene el listado de los catalogos que tiene el proyecto de JAPAMI
    /// y las operaciones. 
    /// PARÁMETROS :
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO : 30/Agosto/2010
    /// MODIFICO   :
    /// FECHA_MODIFICO:
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Bitacora
    {
        public static String[] Array_Catalogos = { "Asuntos", "Areas", "Colonias", "Bitacora de Eventos", "Dependencias", "Tramites", };
        public static String[] Array_Operaciones = new String[] { "Peticiones", "Solicitudes" };
        public const String Accion_Alta = "Alta";
        public const String Accion_Modificar = "Modificar";
        public const String Accion_Consultar = "Consultar";
        public const String Accion_Imprimir = "Imprimir";
        public const String Accion_Eliminar = "Eliminar";
        public const String Accion_Reporte = "Reporte";
        public const String Accion_Estadistica = "Estadistica";
        public const String Accion_Acceso = "Acceso";
        public const String Accion_Baja = "Baja";

    }



    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Roles
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla APL_CAT_ROLES
    /// PARÁMETROS :
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Apl_Cat_Roles
    {
        public const String Tabla_Apl_Cat_Roles = "APL_CAT_ROLES";
        public const String Campo_Rol_ID = "ROL_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Grupo_Roles_ID = "GRUPO_ROLES_ID";
        

    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Apl_Grupos_Roles
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla APL_GRUPOS_ROLES
    /// PARÁMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 06/Oct/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Apl_Grupos_Roles {
        public const String Tabla_Apl_Grupos_Roles = "APL_GRUPOS_ROLES";
        public const String Campo_Grupo_Roles_ID = "GRUPO_ROLES_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Apl_Cat_Accesos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla APL_CAT_ACCESOS
    /// PARÁMETROS :
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Apl_Cat_Accesos
    {
        public const String Tabla_Apl_Cat_Accesos = "APL_CAT_ACCESOS";
        public const String Campo_Menu_ID = "MENU_ID";
        public const String Campo_Rol_ID = "ROL_ID";
        public const String Campo_Habilitado = "HABILITADO";
        public const String Campo_Alta = "ALTA";
        public const String Campo_Cambio = "CAMBIO";
        public const String Campo_Eliminar = "ELIMINAR";
        public const String Campo_Consultar = "CONSULTAR";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Apl_Cat_Menus
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla APL_CAT_MENUS
    /// PARÁMETROS :
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          : Fernando Gonzalez
    /// FECHA_MODIFICO    : 4/Mayo/2012
    /// CAUSA_MODIFICACIÓN: Se agrego la constante del campo Moudlo_ID
    ///*******************************************************************************
    public class Apl_Cat_Menus
    {
        public const String Tabla_Apl_Cat_Menus = "APL_CAT_MENUS";
        public const String Campo_Menu_ID = "MENU_ID";
        public const String Campo_Parent_ID = "PARENT_ID";
        public const String Campo_Menu_Descripcion = "MENU_DESCRIPCION";
        public const String Campo_URL_Link = "URL_LINK";
        public const String Campo_Orden = "ORDEN";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Clasificacion = "CLASIFICACION";
        public const String Campo_Pagina = "PAGINA";
        public const String Campo_Modulo_ID = "MODULO_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Apl_Cat_Modulos_Siag
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla APL_CAT_MODULOS_SIAG
    /// PARÁMETROS :
    /// CREO       : Fernando Gonzalez
    /// FECHA_CREO : 4/Mayo/2012 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Apl_Cat_Modulos_Siag
    {
        public const String Tabla_Apl_Cat_Modulos_Siag = "APL_CAT_MODULOS";
        public const String Campo_Modulo_ID = "MODULO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    #endregion

    ///**********************************************************************************************************************************
    ///                                                                PREDIAL
    ///**********************************************************************************************************************************

    #region Predial
    //*******************************************************************************
    //NOMBRE DE LA CLASE: Cat_Pre_Cajas
    //DESCRIPCIÓN: Clase que contiene los campos de la tabla Cat_Pre_Cajas
    //PARÁMETROS :     
    //CREO       : Miguel Angel Bedolla Moreno.
    //FECHA_CREO : 22/Junio/2011 
    //MODIFICO          : Miguel Angel Bedolla Moreno.
    //FECHA_MODIFICO    : 29/Junio/2011
    //CAUSA_MODIFICACIÓN: La tabla se ha implementado en la base de datos, contiene campos nuevos por lo qu es necesario realizar el ajuste.
    //*******************************************************************************
    public class Cat_Pre_Cajas
    {
        public const String Tabla_Cat_Pre_Caja = "CAT_PRE_CAJAS";
        public const String Campo_Caja_Id = "CAJA_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentario = "COMENTARIOS";
        public const String Campo_Numero_De_Caja = "NO_CAJA";
        public const String Campo_Modulo_Id = "MODULO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    //*******************************************************************************
    //NOMBRE DE LA CLASE: Cat_Pre_Modulos
    //DESCRIPCIÓN: Clase que contiene los campos de la tabla Cat_Pre_Cajas
    //PARÁMETROS :     
    //CREO       : Miguel Angel Bedolla Moreno.
    //FECHA_CREO : 22/Junio/2011 
    //MODIFICO          :
    //FECHA_MODIFICO    :
    //CAUSA_MODIFICACIÓN:
    //*******************************************************************************
    public class Cat_Pre_Modulos
    {
        public const String Tabla_Cat_Pre_Modulo = "CAT_PRE_MODULOS";
        public const String Campo_Modulo_Id = "MODULO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Ubicacion = "UBICACION";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Conceptos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CONCEPTOS
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Conceptos
    {
        public const String Tabla_Cat_Pre_Conceptos = "CAT_PRE_CONCEPTOS";
        public const String Campo_Concepto_Predial_ID = "CONCEPTO_PREDIAL_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Tipo_Concepto = "TIPO_CONCEPTO";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Conceptos_Imp_Predia
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CONCEPTOS_IMP_PREDIA
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Conceptos_Imp_Predia
    {
        public const String Tabla_Cat_Pre_Conceptos_Imp_Predia = "CAT_PRE_CONCEPTOS_IMP_PREDIA";
        public const String Campo_Impuesto_ID_Predial = "IMPUESTO_ID_PREDIAL";
        public const String Campo_Concepto_Predial_ID = "CONCEPTO_PREDIAL_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Tasa = "TASA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Conceptos_Imp_Trasl
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CONCEPTOS_IMP_TRASL
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Conceptos_Imp_Trasl
    {
        public const String Tabla_Cat_Pre_Conceptos_Imp_Trasl = "CAT_PRE_CONCEPTOS_IMP_TRASL";
        public const String Campo_Impuesto_ID_Traslacion = "IMPUESTO_ID_TRASLACION";
        public const String Campo_Concepto_Predial_ID = "CONCEPTO_PREDIAL_ID";
        public const String Campo_Deducible_Normal = "DEDUCIBLE_NORMAL";
        public const String Campo_Deducible_Interes_Social = "DEDUCIBLE_INTERES_SOCIAL";
        public const String Campo_Deducible_20_Salarios = "DEDUCIBLE_20_SALARIOS";
        public const String Campo_Año = "ANIO";
        public const String Campo_Tasa = "TASA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Divisiones
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_DIVISIONES
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Divisiones
    {
        public const String Tabla_Cat_Pre_Divisiones = "CAT_PRE_DIVISIONES";
        public const String Campo_Division_ID = "DIVISION_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Divisiones_Impuestos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_DIVISIONES_IMPUESTOS
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Divisiones_Impuestos
    {
        public const String Tabla_Cat_Pre_Divisiones_Impuestos = "CAT_PRE_DIVISIONES_IMPUESTOS";
        public const String Campo_Impuesto_Division_Lot_ID = "IMPUESTO_DIVISION_LOT_ID";
        public const String Campo_Division_ID = "DIVISION_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Tasa = "TASA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Fraccionamientos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_FRACCIONAMIENTOS
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Fraccionamientos
    {
        public const String Tabla_Cat_Pre_Fraccionamientos = "CAT_PRE_FRACCIONAMIENTOS";
        public const String Campo_Fraccionamiento_ID = "FRACCIONAMIENTO_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Fracc_Impuestos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_FRACC_IMPUESTOS
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Fracc_Impuestos
    {
        public const String Tabla_Cat_Pre_Fracc_Impuestos = "CAT_PRE_FRACC_IMPUESTOS";
        public const String Campo_Impuesto_Fraccionamiento_ID = "IMPUESTO_FRACCIONAMIENTO_ID";
        public const String Campo_Fraccionamiento_ID = "FRACCIONAMIENTO_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Monto = "MONTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Otros_Documentos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_OTROS_DOCUMENTOS
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Otros_Documentos
    {
        public const String Tabla_Cat_Pre_Otros_Documentos = "CAT_PRE_OTROS_DOCUMENTOS";
        public const String Campo_Otro_Documento_ID = "OTRO_DOCUMENTO_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Clave_Generada = "CLAVE_GENERADA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Otros_Doc_Cuotas
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_OTROS_DOC_CUOTAS
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Otros_Doc_Cuotas
    {
        public const String Tabla_Cat_Pre_Otros_Doc_Cuotas = "CAT_PRE_OTROS_DOC_CUOTAS";
        public const String Campo_Otro_Documento_Cuota_ID = "OTRO_DOCUMENTO_CUOTA_ID";
        public const String Campo_Otro_Documento_ID = "OTRO_DOCUMENTO_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Cuota = "CUOTA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Gastos_Ejecucion
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_GASTOS_EJECUCION
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************/
    public class Cat_Pre_Gastos_Ejecucion
    {
        public const String Tabla_Cat_Pre_Gastos_Ejecucion = "CAT_PRE_GASTOS_EJECUCION";
        public const String Campo_Gasto_Ejecucion_ID = "GASTO_EJECUCION_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Gastos_Ejec_Tasas
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_GASTOS_EJEC_TASAS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Gastos_Ejec_Tasas
    {
        public const String Tabla_Cat_Pre_Gastos_Ejec_Tasas = "CAT_PRE_GASTOS_EJEC_TASAS";
        public const String Campo_Gasto_Ejecucion_Tasa_ID = "GASTO_EJECUCION_TASA_ID";
        public const String Campo_Gasto_Ejecucion_ID = "GASTO_EJECUCION_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Tasa = "TASA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Recargos
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_RECARGOS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Recargos
    {
        public const String Tabla_Cat_Pre_Recargos = "CAT_PRE_RECARGOS";
        public const String Campo_Recargo_ID = "RECARGO_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Recargos_Tasas
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_RECARGOS_TASAS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Recargos_Tasas
    {
        public const String Tabla_Cat_Pre_Recargos_Tasas = "CAT_PRE_RECARGOS_TASAS";
        public const String Campo_Recargo_Tasa_ID = "RECARGO_TASA_ID";
        public const String Campo_Recargo_ID = "RECARGO_ID";
        public const String Campo_No_Bimestro = "NO_BIMESTRO";
        public const String Campo_Enero = "ENERO";
        public const String Campo_Febrero = "FEBRERO";
        public const String Campo_Marzo = "MARZO";
        public const String Campo_Abril = "ABRIL";
        public const String Campo_Mayo = "MAYO";
        public const String Campo_Junio = "JUNIO";
        public const String Campo_Julio = "JULIO";
        public const String Campo_Agosto = "AGOSTO";
        public const String Campo_Septiembre = "SEPTIEMBRE";
        public const String Campo_Octubre = "OCTUBRE";
        public const String Campo_Noviembre = "NOVIEMBRE";
        public const String Campo_Diciembre = "DICIEMBRE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Multas
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_MULTAS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Multas
    {
        public const String Tabla_Cat_Pre_Multas = "CAT_PRE_MULTAS";
        public const String Campo_Multa_ID = "MULTA_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Multas_Cuotas
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_MULTAS_CUOTAS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Multas_Cuotas
    {
        public const String Tabla_Cat_Pre_Multas_Cuotas = "CAT_PRE_MULTAS_CUOTAS";
        public const String Campo_Multa_Cuota_ID = "MULTA_CUOTA_ID";
        public const String Campo_Multa_ID = "MULTA_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Monto = "MONTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Descuentos_Predial
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_DESCUENTOS_PREDIAL
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Descuentos_Predial
    {
        public const String Tabla_Cat_Pre_Descuentos_Predial = "CAT_PRE_DESCUENTOS_PREDIAL";
        public const String Campo_Descuento_ID = "DESCUENTO_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Enero = "ENERO";
        public const String Campo_Febrero = "FEBRERO";
        public const String Campo_Marzo = "MARZO";
        public const String Campo_Abril = "ABRIL";
        public const String Campo_Mayo = "MAYO";
        public const String Campo_Junio = "JUNIO";
        public const String Campo_Julio = "JULIO";
        public const String Campo_Agosto = "AGOSTO";
        public const String Campo_Septiembre = "SEPTIEMBRE";
        public const String Campo_Octubre = "OCTUBRE";
        public const String Campo_Noviembre = "NOVIEMBRE";
        public const String Campo_Diciembre = "DICIEMBRE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Descuentos_Recargos
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_DESCUENTOS_RECARGOS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Descuentos_Recargos
    {
        public const String Tabla_Cat_Pre_Descuentos_Recargos = "CAT_PRE_DESCUENTOS_RECARGOS";
        public const String Campo_Descuento_ID = "DESCUENTO_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Enero = "ENERO";
        public const String Campo_Febrero = "FEBRERO";
        public const String Campo_Marzo = "MARZO";
        public const String Campo_Abril = "ABRIL";
        public const String Campo_Mayo = "MAYO";
        public const String Campo_Junio = "JUNIO";
        public const String Campo_Julio = "JULIO";
        public const String Campo_Agosto = "AGOSTO";
        public const String Campo_Septiembre = "SEPTIEMBRE";
        public const String Campo_Octubre = "OCTUBRE";
        public const String Campo_Noviembre = "NOVIEMBRE";
        public const String Campo_Diciembre = "DICIEMBRE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Diferencias
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_DIFERENCIAS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Diferencias
    {
        public const String Tabla_Cat_Pre_Diferencias = "CAT_PRE_DIFERENCIAS";
        public const String Campo_Diferencia_ID = "DIFERENCIA_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Diferencias_Tasas
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_DIFERENCIAS_TASAS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Diferencias_Tasas
    {
        public const String Tabla_Cat_Pre_Diferencias_Tasas = "CAT_PRE_DIFERENCIAS_TASAS";
        public const String Campo_Diferencia_Tasa_ID = "DIFERENCIA_TASA_ID";
        public const String Campo_Diferencia_ID = "DIFERENCIA_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Tasa = "TASA";
        public const String Campo_Tasa_Anual = "TASA_ANUAL";
        public const String Campo_Tasa_Bimestral = "TASA_BIMESTRAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Derechos_Supervision
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CONCEPTOS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Derechos_Supervision
    {
        public const String Tabla_Cat_Pre_Derechos_Supervision = "CAT_PRE_DERECHOS_SUPERVISION";
        public const String Campo_Derecho_Supervision_ID = "DERECHO_SUPERVISION_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Der_Super_Tasas
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_DER_SUPER_TASAS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Der_Super_Tasas
    {
        public const String Tabla_Cat_Pre_Der_Super_Tasas = "CAT_PRE_DER_SUPER_TASAS";
        public const String Campo_Derecho_Supervision_Tasa_ID = "DERECHO_SUPERVISION_TASA_ID";
        public const String Campo_Derecho_Supervision_ID = "DERECHO_SUPERVISION_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Tasa = "TASA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Casos_Especiales
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CASOS_ESPECIALES
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Casos_Especiales
    {
        public const String Tabla_Cat_Pre_Casos_Especiales = "CAT_PRE_CASOS_ESPECIALES";
        public const String Campo_Caso_Especial_ID = "CASO_ESPECIAL_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Articulo = "ARTICULO";
        public const String Campo_Inciso = "INCISO";
        public const String Campo_Aplicar_Descuento = "APLICA_DESCUENTO";
        public const String Campo_Observaciones = "OBSERVACIONES";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Cuotas_Minimas
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CUOTAS_MINIMAS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Cuotas_Minimas
    {
        public const String Tabla_Cat_Pre_Cuotas_Minimas = "CAT_PRE_CUOTAS_MINIMAS";
        public const String Campo_Cuota_Minima_ID = "CUOTA_MINIMA_ID";
        public const String Campo_Año = "ANIO";
        public const String Campo_Cuota = "CUOTA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Bloq_Cuentas_Predial
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_BLOQ_CUENTAS_PREDIAL
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Bloq_Cuentas_Predial
    {
        public const String Tabla_Cat_Pre_Bloq_Cuentas_Predial = "CAT_PRE_BLOQ_CUENTAS_PREDIAL";
        public const String Campo_Bloque_Cuenta_Predial_ID = "BLOQUE_CUENTA_PREDIAL_ID";
        public const String Campo_Cuenta_Predial = "CUENTA_PREDIAL";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Tipo_Bloqueo = "TIPO_BLOQUEO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Movimientos
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_MOVIMIENTOS
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Movimientos
    {
        public const String Tabla_Cat_Pre_Movimientos = "CAT_PRE_MOVIMIENTOS";
        public const String Campo_Movimiento_ID = "MOVIMIENTO_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Traslado = "TRASLADO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Uso_Suelo
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_USO_SUELO
     PARÁMETROS :     
     CREO       : Francisco Antonio Gallardo Castañeda.
     FECHA_CREO : 20/Agosto/2010 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *******************************************************************************/
    public class Cat_Pre_Uso_Suelo
    {
        public const String Tabla_Cat_Pre_Uso_Suelo = "CAT_PRE_USO_SUELO";
        public const String Campo_Uso_Suelo_ID = "USO_SUELO_ID";
        public const String Campo_Identificador = "IDENTIFICADOR";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Estados_Predio
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_ESTADOS_PREDIO
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 28/Octubre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Estados_Predio
    {
        public const String Tabla_Cat_Pre_Estados_Predio = "CAT_PRE_ESTADOS_PREDIO";
        public const String Campo_Estado_Predio_ID = "ESTADO_PREDIO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Tipos_Predio
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_TIPOS_PREDIO
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 28/Octubre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Tipos_Predio
    {
        public const String Tabla_Cat_Pre_Tipos_Predio = "CAT_PRE_TIPOS_PREDIO";
        public const String Campo_Tipo_Predio_ID = "TIPO_PREDIO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Sectores
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_SECTORES
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 29/Octubre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Sectores
    {
        public const String Tabla_Cat_Pre_Sectores = "CAT_PRE_SECTORES";
        public const String Campo_Sector_ID = "SECTOR_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Contribuyentes
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CONTRIBUYENTES
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 13/Septiembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Contribuyentes
    {
        public const String Tabla_Cat_Pre_Contribuyentes = "CAT_PRE_CONTRIBUYENTES";
        public const String Campo_Contribuyente_ID = "CONTRIBUYENTE_ID";
        public const String Campo_Apellido_Paterno = "APELLIDO_PATERNO";
        public const String Campo_Apellido_Materno = "APELLIDO_MATERNO";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Sexo = "SEXO";
        public const String Campo_Estado_Civil = "ESTADO_CIVIL";
        public const String Campo_Fecha_Nacimiento = "FECHA_NACIMIENTO";
        public const String Campo_RFC = "RFC";
        public const String Campo_CURP = "CURP";
        public const String Campo_IFE = "IFE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Tipo_Pesona = "TIPO_PERSONA";
        public const String Campo_Representante_Legal = "REPRESENTANTE_LEGAL";
        public const String Campo_Tipo_Propietario = "TIPO_PROPIETARIO";
        public const String Campo_Domicilio = "DOMICILIO";
        public const String Campo_Interior = "INTERIOR";
        public const String Campo_Exterior = "EXTERIOR";
        public const String Campo_Colonia = "COLONIA";
        public const String Campo_Ciudad = "CIUDAD";
        public const String Campo_Codigo_Postal = "CODIGO_POSTAL";
        public const String Campo_Estado = "ESTADO";
        public const String Campo_Domicilio_Foraneo = "DOMICILIO_FORANEO";
        public const String Campo_Interior_Foraneo = "INTERIOR_FORANEO";
        public const String Campo_Exterior_Foraneo = "EXTERIOR_FORANEO";
        public const String Campo_Colonia_Foraneo = "COLONIA_FORANEO";
        public const String Campo_Ciudad_Foraneo = "CIUDAD_FORANEO";
        public const String Campo_Codigo_Postal_Foraneo = "CODIGO_POSTAL_FORANEO";
        public const String Campo_Estado_Foraneo = "ESTADO_FORANEO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Calles
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CALLES
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 17/Septiembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Calles
    {
        public const String Tabla_Cat_Pre_Calles = "CAT_PRE_CALLES";
        public const String Campo_Calle_ID = "CALLE_ID";
        public const String Campo_Colonia_ID = "Colonia_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Clave = "Clave";
        public const String Campo_Estatus = "Estatus";
        public const String Campo_Comentarios = "Comentarios";
        public const String Campo_Tipo_Calle = "TIPO_CALLE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Calles_Colonias
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CALLES_COLONIAS
    /// PARÁMETROS :     
    /// CREO       : Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO : 17/Septiembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Calles_Colonias
    {
        public const String Tabla_Cat_Pre_Calles_Colonias = "CAT_PRE_CALLES_COLONIAS";
        public const String Campo_Calle_Colonia_ID = "CALLE_COLONIA_ID";
        public const String Campo_Calle_ID = "CALLE_ID";
        public const String Campo_Colonia_ID = "COLONIA_ID";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE : Cat_Pre_Notarios
    /// DESCRIPCIÓN        : Clase que contiene los campos de la tabla CAT_PRE_NOTARIOS
    /// PARÁMETROS         : 
    /// CREO               : Antonio Salvador Benavides Guardado
    /// FECHA_CREO         : 26/Octubre/2010 
    /// MODIFICO           :
    /// FECHA_MODIFICO     :
    /// CAUSA_MODIFICACIÓN :
    ///*******************************************************************************
    public class Cat_Pre_Notarios
    {
        public const String Tabla_Cat_Pre_Notarios = "CAT_PRE_NOTARIOS";
        public const String Campo_Notario_ID = "NOTARIO_ID";
        public const String Campo_Apellido_Paterno = "APELLIDO_PATERNO";
        public const String Campo_Apellido_Materno = "APELLIDO_MATERNO";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Sexo = "SEXO";
        public const String Campo_Estado = "ESTADO";
        public const String Campo_Ciudad = "CIUDAD";
        public const String Campo_Colonia = "COLONIA";
        public const String Campo_Calle = "CALLE";
        public const String Campo_Numero_Exterior = "NUMERO_EXTERIOR";
        public const String Campo_Numero_Interior = "NUMERO_INTERIOR";
        public const String Campo_Codigo_Postal = "CODIGO_POSTAL";
        public const String Campo_RFC = "RFC";
        public const String Campo_CURP = "CURP";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Telefono = "TELEFONO";
        public const String Campo_Fax = "FAX";
        public const String Campo_Celular = "CELULAR";
        public const String Campo_E_Mail = "E_MAIL";
        public const String Campo_Numero_Notaria = "NUMERO_NOTARIA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE : Cat_Pre_Tipos_Colonias
    /// DESCRIPCIÓN        : Clase que contiene los campos de la tabla CAT_PRE_TIPOS_COLONIAS
    /// PARÁMETROS         : 
    /// CREO               : Antonio Salvador Benavides Guardado
    /// FECHA_CREO         : 27/Octubre/2010 
    /// MODIFICO           :
    /// FECHA_MODIFICO     :
    /// CAUSA_MODIFICACIÓN :
    ///*******************************************************************************
    public class Cat_Pre_Tipos_Colonias
    {
        public const String Tabla_Cat_Pre_Tipos_Colonias = "CAT_PRE_TIPOS_COLONIAS";
        public const String Campo_Tipo_Colonia_ID = "TIPO_COLONIA_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE : Cat_Pre_Rangos_Identificadores_Colonias
    /// DESCRIPCIÓN        : Clase que contiene los campos de la tabla CAT_PRE_RANGOS_IDENTIFICADORES_COLONIAS
    /// PARÁMETROS         : 
    /// CREO               : Antonio Salvador Benavides Guardado
    /// FECHA_CREO         : 28/Octubre/2010 
    /// MODIFICO           :
    /// FECHA_MODIFICO     :
    /// CAUSA_MODIFICACIÓN :
    ///*******************************************************************************
    public class Cat_Pre_Rangos_Identificadores_Colonias
    {
        public const String Tabla_Cat_Pre_Rangos_Identificadores_Colonias = "CAT_PRE_RAN_IDENT_COLONIAS";
        public const String Campo_Rango_Identificador_Colonia_ID = "RANGO_IDENTIFICADOR_COLONIA_ID";
        public const String Campo_Tipo_Colonia_ID = "TIPO_COLONIA_ID";
        public const String Campo_Rango_Inicial = "RANGO_INICIAL";
        public const String Campo_Rango_Final = "RANGO_FINAL";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Pre_Contrarecibos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PRE_CONTRARECIBOS
    /// PARÁMETROS :     
    /// CREO       : Franciso Antonio Gallardo Castañeda
    /// FECHA_CREO : 08/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Contrarecibos
    {
        public const String Tabla_Ope_Pre_Contrarecibos = "OPE_PRE_CONTRARECIBOS";
        public const String Campo_No_Contrarecibo = "NO_CONTRARECIBO";
        public const String Campo_Notario_ID = "NOTARIO_ID";
        public const String Campo_Listado_ID = "LISTADO_ID";
        public const String Campo_Cuenta_Predial_ID = "CUENTA_PREDIAL_ID";
        public const String Campo_No_Escritura = "NO_ESCRITURA";
        public const String Campo_Fecha_Escritura = "FECHA_ESCRITURA";
        public const String Campo_Fecha_Liberacion = "FECHA_LIBERACION";
        public const String Campo_Fecha_Pago = "FECHA_PAGO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Pre_Recepcion_Documentos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PRE_RECEPCION_DOCUMENTOS
    /// PARÁMETROS :     
    /// CREO       : Franciso Antonio Gallardo Castañeda
    /// FECHA_CREO : 08/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Recepcion_Documentos
    {
        public const String Tabla_Ope_Pre_Recepcion_Documentos = "OPE_PRE_RECEPCION_DOCUMENTOS";
        public const String Campo_No_Recepcion_Documento = "NO_RECEPCION_DOCUMENTO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Clave_Tramite = "CLAVE_TRAMITE";
        public const String Campo_Notario_ID = "NOTARIO_ID";
        public const String Campo_Observaciones = "OBSERVACIONES";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Pre_Listados
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PRE_LISTADOS
    /// PARÁMETROS :     
    /// CREO       : Franciso Antonio Gallardo Castañeda
    /// FECHA_CREO : 10/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Listados
    {
        public const String Tabla_Ope_Pre_Listados = "OPE_PRE_LISTADOS";
        public const String Campo_Listado_ID = "LISTADO_ID";
        public const String Campo_Notario_ID = "NOTARIO_ID";
        public const String Campo_Fecha_Generacion = "FECHA_GENERACION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Cat_Pre_Cuentas_Predial
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla CAT_PRE_CUENTAS_PREDIAL
    /// PARÁMETROS :     
    /// CREO       			: Antonio Salvador Benavides Guardado
    /// FECHA_CREO 			: 01/Diciembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Cuentas_Predial
    {
        public const String Tabla_Cat_Pre_Cuentas = "CAT_PRE_CUENTAS_PREDIAL";
        public const String Campo_Cuenta_Predial_ID = "CUENTA_PREDIAL_ID";
        public const String Campo_Cuenta_Predial = "CUENTA_PREDIAL";
        public const String Campo_Calle_ID = "CALLE_ID";
        public const String Campo_Propietario_ID = "PROPIETARIO_ID";
        public const String Campo_Copropietario_ID = "COPROPIETARIO_ID";
        public const String Campo_Estado_Predio_ID = "ESTADO_PREDIO_ID";
        public const String Campo_Tipo_Predio_ID = "TIPO_PREDIO_ID";
        public const String Campo_Uso_Suelo_ID = "USO_SUELO_ID";
        public const String Campo_Impuesto_ID_Predial = "IMPUESTO_ID_PREDIAL";
        public const String Campo_Cuota_Minima_ID = "CUOTA_MINIMA_ID";
        public const String Campo_Cuenta_Origen = "CUENTA_ORIGEN";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_No_Exterior = "NO_EXTERIOR";
        public const String Campo_No_Interior = "NO_INTERIOR";
        public const String Campo_Superficie_Construida = "SUPERFICIE_CONSTRUIDA";
        public const String Campo_Superficie_Total = "SUPERFICIE_TOTAL";
        public const String Campo_Clave_Catastral = "CLAVE_CATASTRAL";
        public const String Campo_Valor_Fiscal = "VALOR_FISCAL";
        public const String Campo_Efectos = "EFECTOS";
        public const String Campo_Periodo_Corriente = "PERIODO_CORRIENTE";
        public const String Campo_Cuota_Anual = "CUOTA_ANUAL";
        public const String Campo_Porcentaje_Exencion = "PORCENTAJE_EXENCION";
        public const String Campo_Cuota_Fija = "CUOTA_FIJA";
        public const String Campo_Termino_Exencion = "TERMINO_EXENCION";
        public const String Campo_Fecha_Avaluo = "FECHA_AVALUO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Ope_Pre_Ordenes_Variacion
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla OPE_PRE_ORDENES_VARIACION
    /// PARÁMETROS :     
    /// CREO       			: Antonio Salvador Benavides Guardado
    /// FECHA_CREO 			: 20/Diciembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Ordenes_Variacion
    {
        public const String Tabla_Ope_Pre_Ordenes_Variacion = "OPE_PRE_ORDENES_VARIACION";
        public const String Campo_Orden_Variacion_ID = "ORDEN_VARIACION_ID";
        public const String Campo_Cuenta_Predial_ID = "CUENTA_PREDIAL_ID";
        public const String Campo_Concepto_Predial_ID = "CONCEPTO_PREDIAL_ID";
        public const String Campo_Caso_Especial_ID = "CASO_ESPECIAL_ID";
        public const String Campo_Multa_ID = "MULTA_ID";
        public const String Campo_Cuenta_Predial = "CUENTA_PREDIAL";
        public const String Campo_Valor_Fiscal = "VALOR_FISCAL";
        public const String Campo_Efectos_Año = "EFECTOS_AÑO";
        public const String Campo_Efectos_Bimestre = "EFECTOS_BIMESTRE";
        public const String Campo_Tasa = "TASA";
        public const String Campo_Periodo_Corriente_Inicia = "PERIODO_CORRIENTE_INICIA";
        public const String Campo_Periodo_Corriente_Termina = "PERIODO_CORRIENTE_TERMINA";
        public const String Campo_Cuota_Anual = "CUOTA_ANUAL";
        public const String Campo_Cuota_Bimestral = "CUOTA_BIMESTRAL";
        public const String Campo_Exencion = "EXCENCION";
        public const String Campo_Fecha_Termina_Exencion = "FECHA_TERMINA_EXCENCION";
        public const String Campo_Cuota_Fija = "CUOTA_FIJA";
        public const String Campo_Fecha_Avaluo = "FECHA_AVALUO";
        public const String Campo_Periodo_Rezago = "PERIODO_REZAGO";
        public const String Campo_Fecha_Periodo_Rezago_Inicia = "FECHA_PERIODO_REZAGO_INICIA";
        public const String Campo_Fecha_Periodo_Rezago_Termina = "FECHA_PERIODO_REZAGO_TERMINA";
        public const String Campo_Domicilio_Foraneo = "DOMICILIIO_FORANEO";
        public const String Campo_Predio_Colindante = "PREDIO_COLINDANTE";
        public const String Campo_Base_Impuesto = "BASE_IMPUESTO";
        public const String Campo_Minimo_Elevado_Año = "MINIMO_ELEVADO_AÑO";
        public const String Campo_Tasa_Traslado_Dominio = "TASA_TRASLADO_DOMINIO";
        public const String Campo_Constancia_No_Adeudo = "CONSTANCIA_NO_ADEUDO";
        public const String Campo_Recargos = "RECARGOS";
        public const String Campo_Total = "TOTAL";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Ope_Pre_Observaciones
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla OPE_PRE_OBSERVACIONES
    /// PARÁMETROS :     
    /// CREO       			: Antonio Salvador Benavides Guardado
    /// FECHA_CREO 			: 21/Diciembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Observaciones
    {
        public const String Tabla_Ope_Pre_Observaciones = "OPE_PRE_OBSERVACIONES";
        public const String Campo_Observaciones_ID = "OBSERVACIONES_ID";
        public const String Campo_Orden_Variacion_ID = "ORDEN_VARIACION_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Ope_Pre_Copropietarios_Orde_Variacion
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla OPE_PRE_COPROP_ORDEN_VARIAC
    /// PARÁMETROS :     
    /// CREO       			: Antonio Salvador Benavides Guardado
    /// FECHA_CREO 			: 14/Enero/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Copropietarios_Orde_Variacion
    {
        public const String Tabla_Ope_Pre_Copropietarios_Orden_Variacion = "OPE_PRE_COPROP_ORDEN_VARIAC";
        public const String Campo_Copropietario_Orden_Variacion_ID = "COPROPIETARIO_ORDEN_VARIAC_ID";
        public const String Campo_Orden_Variacion_ID = "ORDEN_VARIACION_ID";
        public const String Campo_Copropietario_ID = "COPROPIETARIO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Cat_Pre_Tipos_Documento
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla CAT_PRE_TIPOS_DOCUMENTO
    /// PARÁMETROS :     
    /// CREO       			: Roberto González Oseguera
    /// FECHA_CREO 			: 22-mar-2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Tipos_Documento
    {
        public const String Tabla_Cat_Pre_Tipos_Documento = "CAT_PRE_TIPOS_DOCUMENTO";
        public const String Campo_Documento_ID = "DOCUMENTO_ID";
        public const String Campo_Nombre_Documento = "NOMBRE_DOCUMENTO";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Ope_Pre_Recep_Docs_Anexos
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla OPE_PRE_RECEP_DOCS_ANEXOS
    /// PARÁMETROS :     
    /// CREO       			: Roberto González Oseguera
    /// FECHA_CREO 			: 24-mar-2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Recep_Docs_Anexos
    {
        public const String Tabla_Ope_Pre_Recep_Docs_Anexos = "OPE_PRE_RECEP_DOCS_ANEXOS";
        public const String Campo_No_Anexo = "NO_ANEXO";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_Ruta = "RUTA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Documento_ID = "DOCUMENTO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Ope_Pre_Recep_Docs_Movs
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla OPE_PRE_RECEP_DOCS_MOVS
    /// PARÁMETROS :     
    /// CREO       			: Roberto González Oseguera
    /// FECHA_CREO 			: 24-mar-2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Recep_Docs_Movs
    {
        public const String Tabla_Ope_Pre_Recep_Docs_Movs = "OPE_PRE_RECEP_DOCS_MOVS";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_No_Recepcion_Documento = "NO_RECEPCION_DOCUMENTO";
        public const String Campo_Numero_Escritura = "NUMERO_ESCRITURA";
        public const String Campo_Fecha_Escritura = "FECHA_ESCRITURA";
        public const String Campo_Cuenta_Predial_ID = "CUENTA_PREDIAL_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Observaciones = "OBSERVACIONES";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_No_Contrarecibo = "NO_CONTRARECIBO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Ope_Pre_Recep_Docs_Observ
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla OPE_PRE_RECEP_DOCS_OBSERV
    /// PARÁMETROS :     
    /// CREO       			: Roberto González Oseguera
    /// FECHA_CREO 			: 18-may-2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Recep_Docs_Observ
    {
        public const String Tabla_Ope_Pre_Recep_Docs_Observ = "OPE_PRE_RECEP_DOCS_OBSERV";
        public const String Campo_No_Observacion = "NO_OBSERVACION";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_Observaciones = "OBSERVACIONES";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Cat_Pre_Propietarios
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla CAT_PRE_PROPIETARIOS
    /// PARÁMETROS :     
    /// CREO       			: Roberto González Oseguera
    /// FECHA_CREO 			: 30-mar-2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Propietarios
    {
        public const String Tabla_Cat_Pre_Propietarios = "CAT_PRE_PROPIETARIOS";
        public const String Campo_Propietario_ID = "PROPIETARIO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Cuenta_Predial_ID = "CUENTA_PREDIAL_ID";
        public const String Campo_Contribuyente_ID = "CONTRIBUYENTE_ID";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Pre_Turnos
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_TURNOS
    /// PARÁMETROS :
    /// CREO       : José Alfredo García Pichardo
    /// FECHA_CREO : 28/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Turnos
    {
        public const String Tabla_Cat_Pre_Turnos = "CAT_PRE_TURNOS";
        public const String Campo_Turno_ID = "TURNO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Hora_Inicio = "HORA_INICIO";
        public const String Campo_Hora_Fin = "HORA_FIN";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Pre_Cajas_Detalles
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_CAT_CAJAS_DETALLES
    /// PARÁMETROS :
    /// CREO       : José Alfredo García Pichardo
    /// FECHA_CREO : 04/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Cajas_Detalles
    {
        public const String Tabla_Ope_Pre_Cajas_Detalles = "OPE_PRE_CAJAS_DETALLES";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Caja_ID = "CAJA_ID";
        public const String Campo_Turno_ID = "TURNO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Cat_Pre_Tipos_Constancias
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla CAT_PRE_TIPOS_CONSTANCIAS
    /// PARÁMETROS :     
    /// CREO       			: Antonion Salvador Benavides Guardado
    /// FECHA_CREO 			: 29/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Pre_Tipos_Constancias
    {
        public const String Tabla_Cat_Pre_Tipos_Constancias = "CAT_PRE_TIPOS_CONSTANCIAS";
        public const String Campo_Tipo_Constancia_ID = "TIPO_CONSTANCIA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Año = "AÑO";
        public const String Campo_Costo = "COSTO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE	: Ope_Pre_Constancias
    /// DESCRIPCIÓN			: Clase que contiene los campos de la tabla OPE_PRE_CONSTANCIAS
    /// PARÁMETROS :     
    /// CREO       			: Antonion Salvador Benavides Guardado
    /// FECHA_CREO 			: 29/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Constancias
    {
        public const String Tabla_Ope_Pre_Constancias = "OPE_PRE_CONSTANCIAS";
        public const String Campo_No_Constancia = "NO_CONSTANCIA";
        public const String Campo_Tipo_Constancia_ID = "TIPO_CONSTANCIA_ID";
        public const String Campo_Cuenta_Predial_ID = "CUENTA_PREDIAL_ID";
        public const String Campo_Propietario_ID = "PROPIETARIO_ID";
        public const String Campo_Realizo = "REALIZO";
        public const String Campo_Confronto = "CONFRONTO";
        public const String Campo_Documento_ID = "DOCUMENTO_ID";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_No_Recibo = "NO_RECIBO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Periodo_Año = "PERIODO_AÑO";
        public const String Campo_Periodo_Bimestre = "PERIODO_BIMESTRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Observaciones = "OBSERVACIONES";
        public const String Campo_Leyenda_Certificacion = "LEYENDA_CERTIFICACION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Grupos
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_GRUPOS
     PARÁMETROS :     
     CREO       : Miguel Angel Bedolla Moreno
     FECHA_CREO : 07/Julio/2011 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *********************************************************************************/
    public class Cat_Pre_Grupos
    {
        public const String Tabla_Cat_Pre_Grupos = "CAT_PRE_GRUPOS";
        public const String Campo_Grupo_ID = "GRUPO_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Rama_ID = "RAMA_ID";
    }

    /*******************************************************************************
     NOMBRE DE LA CLASE: Cat_Pre_Ramas
     DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_RAMAS
     PARÁMETROS :     
     CREO       : Miguel Angel Bedolla Moreno
     FECHA_CREO : 07/Julio/2011 
     MODIFICO          :
     FECHA_MODIFICO    :
     CAUSA_MODIFICACIÓN:
    *********************************************************************************/
    public class Cat_Pre_Ramas
    {
        public const String Tabla_Cat_Pre_Ramas = "CAT_PRE_RAMAS";
        public const String Campo_Rama_ID = "RAMA_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    #endregion

    ///*************************************************************************************************************************
    ///                                                                NOMINA
    ///*************************************************************************************************************************

    #region Nomina

    #region (Catalogos)
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Tab_Orden_Judicial
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_TAB_ORDEN_JUDICIAL
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernández Negrete.
    /// FECHA_CREO : 05/Agosto/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Tab_Orden_Judicial
    {
        public const String Tabla_Cat_Nom_Tab_Orden_Judicial = "CAT_NOM_TAB_ORDEN_JUDICIAL";
        public const String Campo_Orden_Judicial_ID = "ORDEN_JUDICIAL_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Beneficiario = "BENEFICIARIO";
        public const String Campo_Tipo_Descuento_Orden_Judicial_Sueldo = "TIPO_DESC_OJ_SUELDO";
        public const String Campo_Cantidad_Porcentaje_Orden_Judicial_Sueldo = "CANTIDAD_PORC_SUELDO";
        public const String Campo_Bruto_Neto_Orden_Judicial_Sueldo = "OJ_BRUTO_NETO_SUELDO";
        public const String Campo_Tipo_Descuento_Orden_Judicial_Aguinaldo = "TIPO_DESC_OJ_AGUINALDO";
        public const String Campo_Cantidad_Porcentaje_Orden_Judicial_Aguinaldo = "CANTIDAD_PORC_AGUINALDO";
        public const String Campo_Bruto_Neto_Orden_Judicial_Aguinaldo = "OJ_BRUTO_NETO_AGUI";
        public const String Campo_Tipo_Descuento_Orden_Judicial_PV = "TIPO_DESC_OJ_PRIMA_VAC";
        public const String Campo_Cantidad_Porcentaje_Orden_Judicial_PV = "CANTIDAD_PORC_PRIMA_VAC";
        public const String Campo_Bruto_Neto_Orden_Judicial_PV = "OJ_BRUTO_NETO_PV";
        public const String Campo_Tipo_Descuento_Orden_Judicial_Indemnizacion = "TIPO_DESC_OJ_INDEMNIZACION";
        public const String Campo_Cantidad_Porcentaje_Orden_Judicial_Indemnizacion = "CANTIDAD_PORC_INDEMNIZACION";
        public const String Campo_Bruto_Neto_Orden_Judicial_Indemnizacion = "OJ_BRUTO_NETO_INDEMNIZACION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Indemnizacion
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_INDEMNIZACION
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernández Negrete.
    /// FECHA_CREO : 20/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Indemnizacion
    {
        public const String Tabla_Cat_Nom_Indemnizacion = "CAT_NOM_INDEMNIZACION";
        public const String Campo_Indemnizacion_ID = "INDEMNIZACION_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Dias = "DIAS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    public class Cat_Nom_Dep_Puestos_Det
    {
        public const String Tabla_Cat_Nom_Dep_Puestos_Det = "CAT_NOM_DEP_PUESTOS_DET";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Puesto_ID = "PUESTO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Clave = "CLAVE";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Dependencias
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_DEPENDENCIAS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Dependencias
    {
        public const String Tabla_Cat_Dependencias = "CAT_DEPENDENCIAS";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Responsable = "RESPONSABLE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Grupo_Dependencia_ID = "GRUPO_DEPENDENCIA_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Areas
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_AREAS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Areas
    {
        public const String Tabla_Cat_Areas = "CAT_AREAS";
        public const String Campo_Area_ID = "AREA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Responsable = "RESPONSABLE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Turnos
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_TURNOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Turnos
    {
        public const String Tabla_Cat_Turnos = "CAT_TURNOS";
        public const String Campo_Turno_ID = "TURNO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Hora_Entrada = "HORA_ENTRADA";
        public const String Campo_Hora_Salida = "HORA_SALIDA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Empleados
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_EMPLEADOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// 
    ///         [MODIFICACION A]
    /// 
    /// MODIFICO          : Yazmin A. Delgado Gómez
    /// FECHA_MODIFICO    : 05/Noviembre/2010
    /// CAUSA_MODIFICACION: Porque se quito el campo de Grupo_Percepcion_Deduccion_ID
    ///                     ya que el catálogo va hacer eliminado por la composición
    ///                     de la nómina actual
    /// 
    ///         [MODIFICACION B]
    /// 
    /// MODIFICO          : Juan Alberto Hernandez Negrete
    /// FECHA_MODIFICO    : 05/Noviembre/2010
    /// CAUSA_MODIFICACION: Se agregaron dos Referencias:
    ///                     1.- CAT_NOM_TERCEROS        TERCERO_ID
    ///                     2.- CAT_NOM_TIPOS_NOMINAS   TIPO_NOMINA_ID
    ///                     
    ///     Se elimino el campo de TIPO_NOMINA, ya que este campo no hacia referencia a 
    ///     ninguna tabla                
    ///*******************************************************************************
    public class Cat_Empleados
    {
        public const String Tabla_Cat_Empleados = "CAT_EMPLEADOS";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Area_ID = "AREA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Programa_ID = "PROGRAMA_ID";
        public const String Campo_Tipo_Contrato_ID = "TIPO_CONTRATO_ID";
        public const String Campo_Puesto_ID = "PUESTO_ID";
        public const String Campo_Escolaridad_ID = "ESCOLARIDAD_ID";
        public const String Campo_Sindicato_ID = "SINDICATO_ID";
        public const String Campo_Turno_ID = "TURNO_ID";
        public const String Campo_Zona_ID = "ZONA_ID";
        public const String Campo_Tipo_Trabajador_ID = "TIPO_TRABAJADOR_ID";
        public const String Campo_Rol_ID = "ROL_ID";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
        public const String Campo_Terceros_ID = "TERCERO_ID";
        public const String Campo_No_Empleado = "NO_EMPLEADO";
        public const String Campo_Password = "PASSWORD";
        public const String Campo_Apellido_Paterno = "APELLIDO_PATERNO";
        public const String Campo_Apellido_Materno = "APELLIDO_MATERNO";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Calle = "CALLE";
        public const String Campo_Colonia = "COLONIA";
        public const String Campo_Codigo_Postal = "CODIGO_POSTAL";
        public const String Campo_Ciudad = "CIUDAD";
        public const String Campo_Estado = "ESTADO";
        public const String Campo_Telefono_Casa = "TELEFONO_CASA";
        public const String Campo_Telefono_Oficina = "TELEFONO_OFICINA";
        public const String Campo_Extension = "EXTENSION";
        public const String Campo_Fax = "FAX";
        public const String Campo_Celular = "CELULAR";
        public const String Campo_Nextel = "NEXTEL";
        public const String Campo_Correo_Electronico = "CORREO_ELECTRONICO";
        public const String Campo_Sexo = "SEXO";
        public const String Campo_Fecha_Nacimiento = "FECHA_NACIMIENTO";
        public const String Campo_RFC = "RFC";
        public const String Campo_CURP = "CURP";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Ruta_Foto = "RUTA_FOTO";
        public const String Campo_Nombre_Foto = "NOMBRE_FOTO";
        public const String Campo_No_IMSS = "NO_IMSS";
        public const String Campo_Forma_Pago = "FORMA_PAGO";
        public const String Campo_No_Cuenta_Bancaria = "NO_CUENTA_BANCARIA";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Tipo_Finiquito = "TIPO_FINIQUITO";
        public const String Campo_Fecha_Termino_Contrato = "FECHA_TERMINO_CONTRATO";
        public const String Campo_Fecha_Baja = "FECHA_BAJA";
        public const String Campo_Salario_Diario = "SALARIO_DIARIO";
        public const String Campo_Salario_Diario_Integrado = "SALARIO_DIARIO_INTEGRADO";
        public const String Campo_Lunes = "LUNES";
        public const String Campo_Martes = "MARTES";
        public const String Campo_Miercoles = "MIERCOLES";
        public const String Campo_Jueves = "JUEVES";
        public const String Campo_Viernes = "VIERNES";
        public const String Campo_Sabado = "SABADO";
        public const String Campo_Domingo = "DOMINGO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Licencia_Manejo = "NO_LICENCIA_MANEJO";
        public const String Campo_Fecha_Vencimiento_Licencia = "FECHA_VENCIMIENTO_LICENCIA";
        public const String Campo_Nombre_Beneficiario = "NOMBRE_BENEFICIARIO";
        public const String Campo_Cantidad_Porcentaje = "CANTIDAD_PORCENTAJE";
        public const String Campo_Aplica_Orden_Judicial = "APLICA_ORDEN_JUDICIAL";
        public const String Campo_Tipo_Desc_Orden_Judicial = "TIPO_DESC_ORDEN_JUDICIAL";
        public const String Campo_Tipo_Desc_Orden_Judicial_Aguinaldo = "TIPO_DESC_OJ_AGUINALDO";
        public const String Campo_Cantidad_Porcentaje_Aguinaldo = "CANTIDAD_PORC_AGUINALDO";
        public const String Campo_Tipo_Desc_Orden_Judicial_Prima_Vacacional = "TIPO_DESC_OJ_PRIMA_VAC";
        public const String Campo_Cantidad_Porcentaje_Prima_Vacacional = "CANTIDAD_PORC_PRIMA_VAC";
        public const String Campo_Salario_Mensual_Actual = "SALARIO_MENSUAL_ACTUAL";
        public const String Campo_Banco_ID = "BANCO_ID";
        public const String Campo_Reloj_Checador = "RELOJ_CHECADOR";
        public const String Campo_Orden_Judicial_Bruto_Neto_Sueldo_Normal = "OJ_BRUTO_NETO_SN";
        public const String Campo_Orden_Judicial_Bruto_Neto_Aguinaldo = "OJ_BRUTO_NETO_AGUI";
        public const String Campo_Orden_Judicial_Bruto_Neto_Prima_Vacacional = "OJ_BRUTO_NETO_PV";
        public const String Campo_No_Tarjeta = "NO_TARJETA";
        //SAP Código Programático.--------------------------------------------------
        public const String Campo_SAP_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_SAP_Area_Responsable_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_SAP_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_SAP_Unidad_Responsable_ID = "DEPENDENCIA_ID";
        public const String Campo_SAP_Partida_ID = "PARTIDA_ID";
        public const String Campo_SAP_Codigo_Programatico = "CODIGO_PROGRAMATICO";

        public const String Campo_No_Seguro = "NO_SEGURO";
        public const String Campo_Beneficiario = "BENEFICIARIO";
        public const String Campo_Indemnizacion_ID = "INDEMNIZACION_ID";

        public const String Campo_Tipo_Empleado = "TIPO_EMPLEADO";
        public const String Campo_Confronto = "CONFRONTO";
        public const String Campo_Aplica_ISSEG = "APLICA_ISSEG";

        public const String Campo_Reloj_Checador_ID = "RELOJ_CHECADO_ID";
        public const String Campo_Fecha_Inicio_Reloj_Checador = "FECHA_INICIO_RELOJ_CHECADOR";
        public const String Campo_Movimiento = "MOVIMIENTO";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Tipo_Licencia = "TIPO_LICENCIA";

        public const String Campo_No_Secuencia_Turno = "NO_SECUENCIA_TURNO";
        public const String Campo_Tipo_Sangre = "TIPO_SANGRE";
        public const String Campo_NO_Contrato = "NO_CONTRATO";
        public const String Campo_No_Infonavit = "NO_INFONAVIT";
        public const String Campo_Tipo_Descuento_Inf = "TIPO_DESCUENTO_INF";
        public const String Campo_Valor_Descuento_Inf = "VALOR_DESCUENTO_INF";
        public const String Campo_Fecha_Inicio_Descuento_Inf = "FECHA_INICIO_DESCUENTO_INF";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Emp_Perc_Dedu_Deta
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_EMPL_PERC_DEDU_DETA
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernández Negrete
    /// FECHA_CREO : 05/Enero/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Emp_Perc_Dedu_Deta
    {
        public const String Tabla_Cat_Nom_Emp_Perc_Dedu_Det = "CAT_NOM_EMPL_PERC_DEDU_DETA";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Cantidad_Retenida = "CANTIDAD_RETENIDA";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Referencia = "REFERENCIA";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Programas
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_PROGRAMAS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Programas
    {
        public const String Tabla_Cat_Nom_Programas = "CAT_NOM_PROGRAMAS";
        public const String Campo_Programa_ID = "PROGRAMA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Tipos_Contratos
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_TIPOS_CONTRATOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Tipos_Contratos
    {
        public const String Tabla_Cat_Nom_Tipos_Contratos = "CAT_NOM_TIPOS_CONTRATOS";
        public const String Campo_Tipo_Contrato_ID = "TIPO_CONTRATO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Escolaridad
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_ESCOLARIDAD
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Escolaridad
    {
        public const String Tabla_Cat_Nom_Escolaridad = "CAT_NOM_ESCOLARIDAD";
        public const String Campo_Escolaridad_ID = "ESCOLARIDAD_ID";
        public const String Campo_Escolaridad = "ESCOLARIDAD";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Sindicatos
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_SINDICATOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Sindicatos
    {
        public const String Tabla_Cat_Nom_Sindicatos = "CAT_NOM_SINDICATOS";
        public const String Campo_Sindicato_ID = "SINDICATO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Responsable = "RESPONSABLE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Zona_Economica
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_ZONA_ECONOMICA
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          : Yazmin A. Delgado Gómez
    /// FECHA_MODIFICO    : 04/Noviembre/2010
    /// CAUSA_MODIFICACION: ^Porque se agrego el campo de Salario_Diario
    ///*******************************************************************************
    public class Cat_Nom_Zona_Economica
    {
        public const String Tabla_Cat_Nom_Zona_Economica = "CAT_NOM_ZONA_ECONOMICA";
        public const String Campo_Zona_ID = "ZONA_ID";
        public const String Campo_Zona_Economica = "ZONA_ECONOMICA";
        public const String Campo_Salario_Diario = "SALARIO_DIARIO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Tipo_Trabajador
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_TIPO_TRABAJADOR
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Tipo_Trabajador
    {
        public const String Tabla_Cat_Nom_Tipo_Trabajador = "CAT_NOM_TIPO_TRABAJADOR";
        public const String Campo_Tipo_Trabajador_ID = "TIPO_TRABAJADOR_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Percepcion_Deduccion
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_PERCEPCION_DEDUCCION
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          : Yazmin A. Delgado Gómez
    /// FECHA_MODIFICO    : 03/Noviembre/2010
    /// CAUSA_MODIFICACION: Porque las percepciones y las deducciones como son 
    ///                     realizadas actualmente en JAPAMI cuando son calculadas
    ///                     son codificadas directamente en código interno y no por
    ///                     el usuario
    ///*******************************************************************************
    public class Cat_Nom_Percepcion_Deduccion
    {
        public const String Tabla_Cat_Nom_Percepcion_Deduccion = "CAT_NOM_PERCEPCION_DEDUCCION";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Tipo_Asignacion = "TIPO_ASIGNACION";
        public const String Campo_Gravable = "GRAVABLE";
        public const String Campo_Porcentaje_Gravable = "PORCENTAJE_GRAVABLE";
        public const String Campo_Aplicar = "APLICAR";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Aplica_IMSS = "APLICA_IMSS";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Grupos_Percepcion_Deduccion
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_GRUPO_PERC_DEDU
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Grupos_Percepcion_Deduccion
    {
        public const String Tabla_Cat_Nom_Grupo_Perc_Dedu = "CAT_NOM_GRUPOS_PERC_DEDU";
        public const String Campo_Grupo_Percepcion_Deduccion_ID = "GRUPO_PERCEPCION_DEDUCCION_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Detalles_Nom_Grupo_Percepcion_Deduccion
    /// DESCRIPCION: Clase con contiene los datos de la tabla DETALLES_NOM_GRUPO_PERC_DEDU
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Detalles_Nom_Grupo_Percepcion_Deduccion
    {
        public const String Tabla_Detalles_Nom_Grupo_Perc_Dedu = "DETALLES_NOM_GRUPO_PERC_DEDU";
        public const String Campo_Grupo_Percepcion_Deduccion_ID = "GRUPO_PERCEPCION_DEDUCCION_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Detalles_Nom_Empleado_Percepcion_Deduccion
    /// DESCRIPCION: Clase con contiene los datos de la tabla DETALLES_NOM_EMPL_PERC_DEDU
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Detalles_Nom_Empleado_Percepcion_Deduccion
    {
        public const String Tabla_Detalles_Nom_Empl_Perc_Dedu = "DETALLES_NOM_EMPL_PERC_DEDU";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Requisitos_Empleados
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_REQUISITOS_EMPLEADOS
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 19/Octubre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Requisitos_Empleados
    {
        public const String Tabla_Cat_Nom_Requisitos_Empleados = "CAT_NOM_REQUISITOS_EMPLEADO";
        public const String Campo_Requisito_ID = "REQUISITO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Archivo = "ARCHIVO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Calendario_Nominas
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_CALENDARIO_NOMINAS
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 20/Octubre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Calendario_Nominas
    {
        public const String Tabla_Cat_Nom_Calendario_Nominas = "CAT_NOM_CALENDARIO_NOMINAS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Fin = "FECHA_FIN";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Anio = "ANIO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Nominas_Detalles
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_NOMINAS_DETALLES
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 21/Octubre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Nominas_Detalles
    {
        public const String Tabla_Cat_Nom_Nominas_Detalles = "CAT_NOM_NOMINAS_DETALLES";
        public const String Campo_Detalle_Nomina_ID = "DETALLE_NOMINA_ID";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Fin = "FECHA_FIN";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Tabla_Cat_Nom_Periodo_Tipo_Nom
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_PERIODO_TIPO_NOM
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 24/Abril/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Periodo_Tipo_Nom
    {
        public const String Tabla_Cat_Nom_Periodo_Tipo_Nom = "CAT_NOM_PERIODO_TIPO_NOM";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
        public const String Campo_Detalle_Nomina_ID = "DETALLE_NOMINA_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Proveedores
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_PROVEEDORES
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 23/Octubre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Proveedores
    {
        public const String Tabla_Cat_Nom_Proveedores = "CAT_NOM_PROVEEDORES";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_RFC = "RFC";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Calle = "CALLE";
        public const String Campo_Numero = "NUMERO";
        public const String Campo_Colonia = "COLONIA";
        public const String Campo_Codigo_Postal = "CODIGO_POSTAL";
        public const String Campo_Ciudad = "CIUDAD";
        public const String Campo_Estado = "ESTADO";
        public const String Campo_Telefono = "TELEFONO";
        public const String Campo_Fax = "FAX";
        public const String Campo_Contacto = "CONTACTO";
        public const String Campo_Email = "EMAIL";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Aval = "AVAL";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Proveedores_Detalles
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_NOMINAS_DETALLES
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 21/Octubre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Proveedores_Detalles
    {
        public const String Tabla_Cat_Nom_Proveedores_Detalles = "CAT_NOM_PROVEEDORES_DETALLES";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Sindicatos_Percepciones_Deducciones_Detalles
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_SIN_PER_DED_DET
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Sindicatos_Percepciones_Deducciones_Detalles
    {
        public const String Tabla_Cat_Nom_Sin_Per_Ded_Det = "CAT_NOM_SIN_PER_DED_DET";
        public const String Campo_Sindicato_ID = "SINDICATO_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Cantidad = "CANTIDAD";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Tipos_Nominas
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_TIPOS_NOMINAS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Tipos_Nominas
    {
        public const String Tabla_Cat_Nom_Tipos_Nominas = "CAT_NOM_TIPOS_NOMINAS";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
        public const String Campo_Nomina = "NOMINA";
        public const String Campo_Dias_Prima_Vacacional_1 = "DIAS_PRIMA_VACACIONAL_1";
        public const String Campo_Dias_Prima_Vacacional_2 = "DIAS_PRIMA_VACACIONAL_2";
        public const String Campo_Dias_Aguinaldo = "DIAS_AGUINALDO";
        public const String Campo_Dias_Exenta_Prima_Vacacional = "DIAS_EXENTA_PRIMA_VACACIONAL";
        public const String Campo_Dias_Exenta_Aguinaldo = "DIAS_EXENTA_AGUINALDO";
        public const String Campo_Despensa = "DESPENSA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Aplica_ISR = "APLICA_ISR";
        public const String Campo_Actualizar_Salario = "ACTUALIZAR_SALARIO";
        public const String Campo_Dias_Prima_Antiguedad = "DIAS_PRIMA_ANTIGUEDAD";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Tipos_Nomina_Percepciones_Deducciones_Detalles
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_TIP_NOM_PER_DED_DET
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Tipos_Nomina_Percepciones_Deducciones_Detalles
    {
        public const String Tabla_Cat_Nom_Tip_Nom_Per_Ded_Det = "CAT_NOM_TIP_NOM_PER_DED_DET ";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Aplica_Todos = "APLICA_TODOS";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Terceros
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_TERCEROS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Terceros
    {
        public const String Tabla_Cat_Nom_Terceros = "CAT_NOM_TERCEROS";
        public const String Campo_Tercero_ID = "TERCERO_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Contacto = "CONTACTO";
        public const String Campo_Telefono = "TELEFONO";
        public const String Campo_Porcentaje_Retencion = "PORCENTAJE_RETENCION";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Parametros
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_PARAMETROS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Cat_Nom_Parametros
    {
        public const String Tabla_Cat_Nom_Parametros = "CAT_NOM_PARAMETROS";
        public const String Campo_Parametro_ID = "PARAMETRO_ID";
        public const String Campo_Zona_ID = "ZONA_ID";
        public const String Campo_Percepcion_Quinquenio = "PERCEPCION_QUINQUENIO";
        public const String Campo_Percepcion_Prima_Vacacional = "PERCEPCION_PRIMA_VACACIONAL";
        public const String Campo_Percepcion_Prima_Dominical = "PERCEPCION_PRIMA_DOMINICAL";
        public const String Campo_Percepcion_Aguinaldo = "PERCEPCION_AGUINALDO";
        public const String Campo_Percepcion_Dias_Festivos = "PERCEPCION_DIAS_FESTIVOS";
        public const String Campo_Percepcion_Horas_Extra = "PERCEPCION_HORAS_EXTRA";
        public const String Campo_Percepcion_Dia_Doble = "PERCEPCION_DIA_DOBLE";
        public const String Campo_Percepcion_Dia_Domingo = "PERCEPCION_DIA_DOMINGO";
        public const String Campo_Percepcion_Ajuste_ISR = "PERCEPCION_AJUSTE_ISR";
        public const String Campo_Percepcion_Incapacidades = "PERCEPCION_INCAPACIDADES";
        public const String Campo_Percepcion_Subsidio = "PERCEPCION_SUBSIDIO";
        public const String Campo_Percepcion_Despensa = "PERCEPCION_DESPENSA";
        public const String Campo_Percepcion_Sueldo_Normal = "PERCEPCION_SUELDO_NORMAL";
        public const String Campo_Percepcion_Prima_Antiguedad = "PERCEPCION_PRIMA_ANTIGUEDAD";
        public const String Campo_Percepcion_Indemnizacion = "PERCEPCION_INDEMNIZACION";
        public const String Campo_Percepcion_Vacaciones_Pendientes_Pagar = "PERCEPCION_VAC_PEND_PAGAR";
        public const String Campo_Percepcion_Vacaciones = "PERCEPCION_VACACIONES";
        public const String Campo_Percepcion_Fondo_Retiro = "PERCEPCION_FONDO_RETIRO";
        public const String Campo_Percepcion_Prevision_Social_Multiple = "PERCEPCION_PSM";

        public const String Campo_Deduccion_Faltas = "DEDUCCION_FALTAS";
        public const String Campo_Deduccion_Retardos = "DEDUCCION_RETARDOS";
        public const String Campo_Deduccion_Fondo_Retiro = "DEDUCCION_FONDO_RETIRO";
        public const String Campo_Deduccion_ISR = "DEDUCCION_ISR";
        public const String Campo_Deduccion_IMSS = "DEDUCCION_IMSS";
        public const String Campo_Deduccion_Tipo_Desc_Orden_Judicial = "DEDUCCION_ORDEN_JUDICIAL";
        public const String Campo_Deduccion_Vacaciones_Tomadas_Mas = "DEDUCCION_VAC_TOMADAS_MAS";
        public const String Campo_Deduccion_Aguinaldo_Pagado_Mas = "DEDUCCION_AGUINALDO_PAGADO_MAS";
        public const String Campo_Deduccion_Prima_Vacacional_Pagada_Mas = "DEDUCCION_PRIMA_VAC_PAGADA_MAS";
        public const String Campo_Deduccion_Sueldo_Pagado_Mas = "DEDUCCION_SUELDO_PAGADO_MAS";
        public const String Campo_Deduccion_Orden_Judicial_Aguinaldo = "DEDUCCION_OJ_AGUINALDO";
        public const String Campo_Deduccion_Orden_Judicial_Prima_Vacacional = "DEDUCCION_OJ_PRIMA_VACACIONAL";
        public const String Campo_Deduccion_Orden_Judicial_Indemnizacion = "DEDUCCION_OJ_INDEMNIZACION";
        public const String Campo_Deduccion_ISSEG = "DEDUCCION_ISSEG";

        public const String Campo_Porcentaje_Prima_Vacacional = "PORCENTAJE_PRIMA_VACACIONAL";
        public const String Campo_Porcentaje_Fondo_Retiro = "PORCENTAJE_FONDO_RETIRO";
        public const String Campo_Porcentaje_Prima_Dominical = "PORCENTAJE_PRIMA_DOMINICAL";
        public const String Campo_Fecha_Prima_Vacacional_1 = "FECHA_PRIMA_VACACIONAL_1";
        public const String Campo_Fecha_Prima_Vacacional_2 = "FECHA_PRIMA_VACACIONAL_2";
        public const String Campo_Salario_Limite_Prestamo = "SUELDO_LIMITE_PRESTAMO";
        public const String Campo_Salario_Mensual_Maximo = "SALARIO_MENSUAL_MAXIMO";
        public const String Campo_Salario_Diario_Int_Topado = "SALARIO_DIARIO_INT_TOPADO";

        public const String Campo_IP_Servidor = "IP_SERVIDOR";
        public const String Campo_Nombre_Base_Datos = "NOMBRE_BASE_DATOS";
        public const String Campo_Usuario_SQL = "USUARIO_SQL";
        public const String Campo_Password_Base_Datos = "PASSWORD_BASE_DATOS";

        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

        public const String Campo_Tipo_IMSS = "TIPO_IMSS";
        public const String Campo_Minutos_Dia = "MINUTOS_DIA";
        public const String Campo_Minutos_Retardo = "MINUTOS_RETARDO";
        public const String Campo_ISSEG_Porcentaje_Prevision_Social_Multiple = "PORCENTAJE_PSM";
        public const String Campo_ISSEG_Porcentaje_Aplicar_Empleado = "PORCENTAJE_FACTOR_SOCIAL";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Emp_Movimientos_Det
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_EMP_MOVIMIENTOS_DET
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 11/Diciembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Emp_Movimientos_Det
    {
        public const String Tabla_Cat_Emp_Movimientos_Det = "CAT_EMP_MOVIMIENTOS_DET";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Puesto_ID = "PUESTO_ID";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
        public const String Campo_Tipo_Movimiento = "TIPO_MOVIMIENTO";
        public const String Campo_Motivo_Movimiento = "MOTIVO_MOVIMIENTO";
        public const String Campo_Sueldo_Actual = "SUELDO_ACTUAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Antiguedad_Sindicato
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_ANTIGUEDAD_SINDICATO
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 24/Diciembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Antiguedad_Sindicato
    {
        public const String Tabla_Cat_Nom_Antiguedad_Sindicato = "CAT_NOM_ANTIGUEDAD_SINDICATO";
        public const String Campo_Antiguedad_Sindicato_ID = "ANTIGUEDAD_SINDICATO_ID";
        public const String Campo_Anios = "ANIOS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Ant_Sin_Det 
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_ANT_SIN_DET
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 24/Diciembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Ant_Sin_Det
    {
        public const String Tabla_Cat_Nom_Ant_Sin_Det = "CAT_NOM_ANT_SIN_DET";
        public const String Campo_Sindicato_ID = "SINDICATO_ID";
        public const String Campo_Antiguedad_Sindicato_ID = "ANTIGUEDAD_SINDICATO_ID";
        public const String Campo_Monto = "MONTO";
    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: CAT_NOM_BANCOS
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla CAT_NOM_BANCOS
    ///
    ///CREO: Juan Alberto Hernández Negrete
    ///FECHA CREÓ: 16/Febrero/2011 17:33 pm.
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Cat_Nom_Bancos
    {
        public const String Tabla_Cat_Nom_Bancos = "CAT_NOM_BANCOS";
        public const String Campo_Banco_ID = "BANCO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_No_Cuenta = "NO_CUENTA";
        public const String Campo_Sucursal = "SUCURSAL";
        public const String Campo_Referencia = "REFERENCIA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Plan_Pago = "PLAN_PAGO";
        public const String Campo_No_Meses = "NO_MESES";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Folio_Inicial = "FOLIO_INICIAL";
        public const String Campo_Folio_Final = "FOLIO_FINAL";
        public const String Campo_Folio_Actual = "FOLIO_ACTUAL";
        public const String Campo_Comision_Transferencia = "COMISION_TRANSFERENCIA";
        public const String Campo_IVA_Comision = "IVA_COMISION";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Banco_Clabe = "BANCO_CLABE";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Reloj_Checador
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_RELOJ_CHECADOR
    /// PARAMETROS :
    /// CREO       : Yazmin A Delgado Gómez
    /// FECHA_CREO : 14/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Reloj_Checador
    {
        public const String Tabla_Cat_Nom_Reloj_Checador = "CAT_NOM_RELOJ_CHECADOR";
        public const String Campo_Reloj_Checador_ID = "RELOJ_CHECADOR_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Ubicacion = "UBICACION";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Nom_Calendario_Reloj
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_NOM_CALENDARIO_RELOJ
    /// PARAMETROS :
    /// CREO       : Yazmin A Delgado Gómez
    /// FECHA_CREO : 16/Agosto/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Nom_Calendario_Reloj
    {
        public const String Tabla_Cat_Nom_Calendario_Reloj = "CAT_NOM_CALENDARIO_RELOJ";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Fin = "FECHA_FIN";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Consecutivo = "CONSECUTIVO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    #endregion

    #region (Operacion [Control Incidencias de los Empleados JAPAMI])
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Asistencias
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_ASISTENCIAS
    /// PARAMETROS :
    /// CREO       : Yazmin A Delgado Gómez
    /// FECHA_CREO : 14/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Asistencias
    {
        public const String Tabla_Ope_Nom_Asistencias = "OPE_NOM_ASISTENCIAS";
        public const String Campo_No_Asistencia = "NO_ASISTENCIA";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Reloj_Checador_ID = "RELOJ_CHECADOR_ID";
        public const String Campo_Fecha_Hora_Entrada = "FECHA_HORA_ENTRADA";
        public const String Campo_Fecha_Hora_Salida = "FECHA_HORA_SALIDA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Incidencias_Checadas
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_INCIDENCIAS_CHECADAS
    /// PARAMETROS :
    /// CREO       : Yazmin A Delgado Gómez
    /// FECHA_CREO : 04/Agosto/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Incidencias_Checadas
    {
        public const String Tabla_Ope_Nom_Incidencias_Checadas = "OPE_NOM_INCIDENCIAS_CHECADAS";
        public const String Campo_No_Incidencia_Checada = "NO_INCIDENCIA_CHECADA";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Reloj_Checador_ID = "RELOJ_CHECADOR_ID";
        public const String Campo_Fecha_Hora_Checada = "FECHA_HORA_CHECADA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Horarios_Empleados
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_ASISTENCIAS
    /// PARAMETROS :
    /// CREO       : Yazmin A Delgado Gómez
    /// FECHA_CREO : 14/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Horarios_Empleados
    {
        public const String Tabla_Ope_Nom_Horarios_Empleados = "OPE_NOM_HORARIOS_EMPLEADOS";
        public const String Campo_No_Horario_Empleado = "NO_HORARIO_EMPLEADO";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Termino = "FECHA_TERMINO";
        public const String Campo_Hora_Entrada = "HORA_ENTRADA";
        public const String Campo_Hora_Salida = "HORA_SALIDA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: Ope_Nom_Nomina_Generada
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_NOM_NOMINA_GENERADA
    ///
    ///CREO: Juan Alberto Hernández Negrete.
    ///FECHA CREÓ: 12/Junio/2011.
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Ope_Nom_Nomina_Generada
    {
        public const String Tabla_Ope_Nom_Nomina_Generada = "OPE_NOM_NOMINA_GENERADA";
        public const String Campo_Nom_Generada_ID = "NOM_GENERADA_ID";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Nomina_Generada = "NOMINA_GENERADA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: Ope_Nom_Generar_Arch_Bancos
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_NOM_GENERAR_ARCH_BANCOS
    ///
    ///CREO: Juan Alberto Hernández Negrete.
    ///FECHA CREÓ: 20/Abril/2011.
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Ope_Nom_Generar_Arch_Bancos
    {
        public const String Tabla_Ope_Nom_Generar_Arch_Bancos = "OPE_NOM_GENERAR_ARCH_BANCOS";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_Banco_ID = "BANCO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: Cat_Nom_Plazas
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla CAT_NOM_PLAZAS
    ///
    ///CREO: Francisco Antonio Gallardo Castañeda
    ///FECHA CREÓ: 04/Abril/2011 11:21 am.
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Cat_Nom_Plazas
    {
        public const String Tabla_Cat_Nom_Plazas = "CAT_NOM_PLAZAS";
        public const String Campo_Plaza_ID = "PLAZA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: Ope_Nom_Actualizar_Salario
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_NOM_ACTUALIZAR_SALARIO
    ///CREO: Francisco Antonio Gallardo Castañeda
    ///FECHA CREÓ: 06/Abril/2011 4:43 pm.
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Ope_Nom_Actualizar_Salario
    {
        public const String Tabla_Ope_Nom_Actualizar_Salario = "OPE_NOM_ACTUALIZAR_SALARIO";
        public const String Campo_No_Actualizar_Salario = "NO_ACTUALIZAR_SALARIO";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Fecha_Actualizacion = "FECHA_ACTUALIZACION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: Ope_Nom_Act_Sal_Det
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_NOM_ACT_SAL_DET
    ///CREO: Francisco Antonio Gallardo Castañeda
    ///FECHA CREÓ: 06/Abril/2011 4:51 pm.
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Ope_Nom_Act_Sal_Det
    {
        public const String Tabla_Ope_Nom_Act_Sal_Det = "OPE_NOM_ACT_SAL_DET";
        public const String Campo_Act_Salario_Detalle_ID = "ACT_SALARIO_DETALLE_ID";
        public const String Campo_No_Actualizar_Salario = "NO_ACTUALIZAR_SALARIO";
        public const String Campo_Sindicato_ID = "SINDICATO_ID";
    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: Ope_Nom_Proveedores
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_NOM_PROVEEDORES
    ///CREO: Francisco Antonio Gallardo Castañeda
    ///FECHA CREÓ: 22/Abril/2011 8:19 am.
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Ope_Nom_Proveedores
    {
        public const String Tabla_Ope_Nom_Proveedores = "OPE_NOM_PROVEEDORES";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina_Inicia = "NO_NOMINA_INICIA";
        public const String Campo_No_Periodos = "NO_PERIODOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///****************************************************************************************************************************************************************
    ///NOMBRE: Ope_Nom_Proveedores_Detalles
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_NOM_PROVEEDORES_DETALLES
    ///CREO: Francisco Antonio Gallardo Castañeda
    ///FECHA CREÓ: 22/Abril/2011 8:27 am.
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Ope_Nom_Proveedores_Detalles
    {
        public const String Tabla_Ope_Nom_Proveedores_Detalles = "OPE_NOM_PROVEEDORES_DETALLES";
        public const String Campo_No_Movimiento_Detalle = "NO_MOVIMIENTO_DETALLE";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_No_Fonacot = "NO_FONACOT";
        public const String Campo_RFC = "RFC";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_No_Credito = "NO_CREDITO";
        public const String Campo_Retencion_Mensual = "RETENCION_MENSUAL";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Plazo = "PLAZO";
        public const String Campo_Cuotas_Pagadas = "CUOTAS_PAGADAS";
        public const String Campo_Retencion_Real = "RETENCION_REAL";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_Periodo = "PERIODO";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Estatus = "ESTATUS";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Puestos
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_PUESTOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Puestos
    {
        public const String Tabla_Cat_Puestos = "CAT_PUESTOS";
        public const String Campo_Puesto_ID = "PUESTO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Salario_Mensual = "SALARIO_MENSUAL";
        public const String Campo_Plaza_ID = "PLAZA_ID";
        public const String Campo_Aplica_Fondo_Retiro = "APLICA_FONDO_RETIRO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Domingos
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_DOMINGOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Domingos_Empleado
    {
        public const String Tabla_Cat_Nom_Ope_Nom_Domingos = "OPE_NOM_DOMINGOS";
        public const String Campo_No_Domingo = "NO_DOMINGO";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Domingos_Empleado_Detalles
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_DOMINGOS_EMP_DET
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 12/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Domingos_Empleado_Detalles
    {
        public const String Tabla_Ope_Nom_Domingos_Empleados_Detalles = "OPE_NOM_DOMINGOS_EMP_DET";
        public const String Campo_No_Domingo = "NO_DOMINGO";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios_Estatus = "COMENTARIOS_ESTATUS";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Faltas_Empleado
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_FALTAS_EMPLEADO
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Faltas_Empleado
    {
        public const String Tabla_Ope_Nom_Faltas_Empleado = "OPE_NOM_FALTAS_EMPLEADO";
        public const String Campo_No_Falta = "NO_FALTA";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Tipo_Falta = "TIPO_FALTA";
        public const String Campo_Retardo = "RETARDO";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Estatus = "ESTATUS";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Tiempo_Extra
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_TIEMPO_EXTRA
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Tiempo_Extra
    {
        public const String Tabla_Ope_Nom_Tiempo_Extra = "OPE_NOM_TIEMPO_EXTRA";
        public const String Campo_No_Tiempo_Extra = "NO_TIEMPO_EXTRA";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Pago_Dia_Doble = "PAGO_DIA_DOBLE";
        public const String Campo_Horas = "HORAS";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Tiempo_Extra_Emp_Det
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_TIEMPO_EXTRA_EMP_DET
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 18/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Tiempo_Extra_Emp_Det
    {
        public const String Tabla_Ope_Nom_Tiempo_Extra = "OPE_NOM_TIEMPO_EXTRA_EMP_DET";
        public const String Campo_No_Tiempo_Extra = "NO_TIEMPO_EXTRA";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios_Estatus = "COMENTARIOS_ESTATUS";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Dias_Festivos_Empleado
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_DIAS_FESTIVOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Dias_Festivos
    {
        public const String Tabla_Ope_Nom_Dias_Festivos = "OPE_NOM_DIAS_FESTIVOS";
        public const String Campo_No_Dia_Festivo = "NO_DIA_FESTIVO";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Dia_ID = "DIA_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Dias_Festivo_Emp_Det 
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_DIAS_FESTIVO_EMP_DET
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 22/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Dias_Festivo_Emp_Det
    {
        public const String Tabla_Ope_Nom_Dias_Festivos_Emp_Det = "OPE_NOM_DIAS_FESTIVO_EMP_DET";
        public const String Campo_No_Dia_Festivo = "NO_DIA_FESTIVO";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios_Estatus = "COMENTARIOS_ESTATUS";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Vacaciones_Empleado
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_VACACIONES_EMPLEADO
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 03/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class OPE_NOM_VACACIONES_EMPLEADO
    {
        public const String Tabla_Ope_Nom_Vacaciones_Empleado = "OPE_NOM_VACACIONES_EMPLEADO";
        public const String Campo_No_Vacacion = "NO_VACACION";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Termino = "FECHA_TERMINO";
        public const String Campo_Cantidad_Dias = "CANTIDAD_DIAS";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios_Estatus = "COMENTARIOS_ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Vacaciones_Empleado_Detalles 
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_VACACIONES_EMPL_DET
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernández Negrete
    /// FECHA_CREO : 1/Marzo/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Vacaciones_Empleado_Detalles
    {
        public const String Tabla_Ope_Nom_Vacaciones_Empl_Det = "OPE_NOM_VACACIONES_EMPL_DET";
        public const String Campo_No_Vacacion_Detalle = "NO_VACACION_DETALLE";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Dias_Disponibles = "DIAS_DISPONIBLES";
        public const String Campo_Dias_Tomados = "DIAS_TOMADOS";
        public const String Campo_Periodo_Vacacional = "PERIODO_VACACIONAL";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Vacaciones_Dia_Det 
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_VACACIONES_DIA_DET
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 30/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Vacaciones_Dia_Det
    {
        public const String Tabla_Ope_Nom_Vacaciones_Dia_Det = "OPE_NOM_VACACIONES_DIA_DET";
        public const String Campo_No_Dia_Vacacion = "NO_DIA_VACACION";
        public const String Campo_No_Vacacion = "NO_VACACION";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Estado = "ESTADO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Percepciones_Var
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_PERCEPCIONES_VAR
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 28/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Percepciones_Var
    {
        public const String Tabla_Ope_Nom_Percepciones_Var = "OPE_NOM_PERCEPCIONES_VAR";
        public const String Campo_No_Percepcion = "NO_PERCEPCION";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Perc_Var_Emp_Det
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_PERC_VAR_EMP_DET
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 28/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Perc_Var_Emp_Det
    {
        public const String Tabla_Ope_Nom_Perc_Var_Emp_Det = "OPE_NOM_PERC_VAR_EMP_DET";
        public const String Campo_No_Percepcion = "NO_PERCEPCION";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios_Estatus = "COMENTARIOS_ESTATUS";
        public const String Campo_Cantidad = "CANTIDAD";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Deducciones_Var
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_DEDUCCIONES_VAR
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 29/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Deducciones_Var
    {
        public const String Tabla_Ope_Nom_Deducciones_Var = "OPE_NOM_DEDUCCIONES_VAR";
        public const String Campo_No_Deduccion = "NO_DEDUCCION";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Referencia = "REFERENCIA";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Deduc_Var_Emp_Det 
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_DEDUC_VAR_EMP_DET
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 29/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Deduc_Var_Emp_Det
    {
        public const String Tabla_Ope_Nom_Deduc_Var_Emp_Det = "OPE_NOM_DEDUC_VAR_EMP_DET";
        public const String Campo_No_Deduccion = "NO_DEDUCCION";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios_Estatus = "COMENTARIOS_ESTATUS";
        public const String Campo_Cantidad = "CANTIDAD";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Requisitos_Empleado
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_REQUISITOS_EMPLEADO
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 27/Octubre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///******************************************************************************
    public class Ope_Nom_Requisitos_Empleado
    {
        public const String Tabla_Ope_Nom_Requisitos_Empleado = "OPE_NOM_REQUISITOS_EMPLEADO";
        public const String Campo_Requisitos_Empleado_ID = "REQUISITO_EMPLEADO_ID";
        public const String Campo_Requisitos_ID = "REQUISITO_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Ruta_Documento = "RUTA_DOCUMENTO";
        public const String Campo_Entregado = "ENTREGADO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Recibos_Empleados
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_RECIBOS_EMPLEADOS
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 14/Diciembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Recibos_Empleados
    {
        public const String Tabla_Ope_Nom_Recibos_Empleados = "OPE_NOM_RECIBOS_EMPLEADOS";
        public const String Campo_No_Recibo = "NO_RECIBO";
        public const String Campo_Detalle_Nomina_ID = "DETALLE_NOMINA_ID";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Puesto_ID = "PUESTO_ID";
        public const String Campo_Dias_Trabajados = "DIAS_TRABAJADOS";
        public const String Campo_Total_Percepciones = "TOTAL_PERCEPCIONES";
        public const String Campo_Total_Deducciones = "TOTAL_DEDUCCIONES";
        public const String Campo_Total_Nomina = "TOTAL_NOMINA";
        public const String Campo_Gravado = "GRAVADO";
        public const String Campo_Exento = "EXENTO";
        public const String Campo_Salario_Diario = "SALARIO_DIARIO";
        public const String Campo_Salario_Diario_Integrado = "SALARIO_DIARIO_INTEGRADO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Nomina_Generada = "NOMINA_GENERADA";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Recibos_Empleados_Det 
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_RECIBOS_EMPLEADOS_DET
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 14/Diciembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Recibos_Empleados_Det
    {
        public const String Tabla_Ope_Nom_Recibos_Empleados_Det = "OPE_NOM_RECIBO_EMPLEADO_DET";
        public const String Campo_No_Recibo_Detalles = "NO_RECIBO_DETALLES";
        public const String Campo_No_Recibo = "NO_RECIBO";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Monto = "MONTO";
        public const String Campo_Gravado = "GRAVADO";
        public const String Campo_Exento = "EXENTO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Totales_Nomina
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_TOTALES_NOMINA
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 14/Diciembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Totales_Nomina
    {
        public const String Tabla_Ope_Nom_Totales_Nomina = "OPE_NOM_TOTALES_NOMINA";
        public const String Campo_No_Total_Nomina = "NO_TOTAL_NOMINA";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_Detalle_Nomina_ID = "DETALLE_NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Tipo_Nomina_ID = "TIPO_NOMINA_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Solicitud_Prestamo 
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_SOLICITUD_PRESTAMO
    /// PARAMETROS :
    /// CREO       : Juan ALberto Hernandez Negrete
    /// FECHA_CREO : 30/Noviembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Solicitud_Prestamo
    {
        public const String Tabla_Ope_Nom_Solicitud_Prestamo = "OPE_NOM_SOLICITUD_PRESTAMO";
        public const String Campo_No_Solicitud = "NO_SOLICITUD";
        public const String Campo_Solicita_Empleado_ID = "SOLICITA_EMPLEADO_ID";
        public const String Campo_Aval_Empleado_ID = "AVAL_EMPLEADO_ID";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Estatus_Solicitud = "ESTATUS_SOLICITUD";
        public const String Campo_Estatus_Pago = "ESTATUS_PAGO";
        public const String Campo_Fecha_Solicitud = "FECHA_SOLICITUD";
        public const String Campo_Fecha_Autorizacion = "FECHA_AUTORIZACION";
        public const String Campo_Fecha_Inicio_Pago = "FECHA_INICIO_PAGO";
        public const String Campo_Fecha_Termino_Pago = "FECHA_TERMINO_PAGO";
        public const String Campo_Motivo_Prestamo = "MOTIVO_PRESTAMO";
        public const String Campo_No_Pagos = "NO_PAGOS";
        public const String Campo_Importe_Prestamo = "IMPORTE_PRESTAMO";
        public const String Campo_Importe_Interes = "IMPORTE_INTERES";
        public const String Campo_Total_Prestamo = "TOTAL_PRESTAMO";
        public const String Campo_Monto_Abonado = "MONTO_ABONADO";
        public const String Campo_Saldo_Actual = "SALDO_ACTUAL";
        public const String Campo_Abono = "ABONO";
        public const String Campo_No_Abono = "NO_ABONO";
        public const String Campo_Comentarios_Cancelacion = "COMENTARIOS_CANCELACION";
        public const String Campo_Comentarios_Rechazo = "COMENTARIOS_RECHAZO";
        public const String Campo_Referencia_Recibo_Pago = "REFERENCIA_RECIBO_PAGO";
        public const String Campo_Aplica_Validaciones = "APLICA_VALIDACIONES";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Fecha_Finiquito_Prestamo = "FECHA_FINIQUITO_PRESTAMO";
        public const String Campo_Estado_Prestamo = "ESTADO_PRESTAMO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Ajuste_ISR
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_AJUSTE_ISR
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 14/Diciembre/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Ajuste_ISR
    {
        public const String Tabla_Ope_Nom_Ajuste_ISR = "OPE_NOM_AJUSTE_ISR";
        public const String Campo_No_Ajuste_ISR = "NO_AJUSTE_ISR";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Estatus_Ajuste_ISR = "ESTATUS_AJUSTE_ISR";
        public const String Campo_Fecha_Inicio_Pago = "FECHA_INICIO_PAGO";
        public const String Campo_Fecha_Termino_Pago = "FECHA_TERMINO_PAGO";
        public const String Campo_Total_ISR_Ajustar = "TOTAL_ISR_AJUSTAR";
        public const String Campo_Total_ISR_Ajustado = "TOTAL_ISR_AJUSTADO";
        public const String Campo_No_Catorcenas = "NO_CATORCENAS";
        public const String Campo_Pago_Catorcenal_ISR = "PAGO_CATORCENAL_ISR";
        public const String Campo_No_Pago = "NO_PAGO";
        public const String Campo_Comentarios_Ajuste = "COMENTARIOS_AJUSTE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Incapacidades
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_INCAPACIDADES
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 06/Abril/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Incapacidades
    {
        public const String Tabla_Ope_Nom_Incapacidades = "OPE_NOM_INCAPACIDADES";
        public const String Campo_No_Incapacidad = "NO_INCAPACIDAD";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Tipo_Incapacidad = "TIPO_INCAPACIDAD";
        public const String Campo_Aplica_Pago_Cuarto_Dia = "APLICA_PAGO_CUARTO_DIA";
        public const String Campo_Porcentaje_Incapacidad = "PORCENTAJE_INCAPACIDAD";
        public const String Campo_Extencion_Incapacidad = "EXTENCION_INCAPACIDAD";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Fin = "FECHA_FIN";
        public const String Campo_Comentario = "COMENTARIOS";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Dias_Incapacidad = "DIAS_INCAPACIDAD";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Nom_Reloj_Checador
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_NOM_RELOJ_CHECADOR
    /// PARAMETROS :
    /// CREO       : Juan Alberto Hernandez Negrete
    /// FECHA_CREO : 19/Abril/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Nom_Reloj_Checador
    {
        public const String Tabla_Ope_Nom_Reloj_Checador = "OPE_NOM_RELOJ_CHECADOR";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fecha_Subio_Informacion = "FECHA_SUBIO_INF";
        public const String Campo_Nomina_ID = "NOMINA_ID";
        public const String Campo_No_Nomina = "NO_NOMINA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Dias_Incapacidad = "DIAS_INCAPACIDAD";
    }
    #endregion

    #region (Tabuladores ISR, Subsidio, Antiguedad, Vacaciones, Dias Festivos)
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: His_Nom_Antiguedad_Empleados
    /// DESCRIPCION: Clase con contiene los datos de la tabla HIS_NOM_ANTIGUEDAD_EMPLEADOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class His_Nom_Antiguedad_Empleados
    {
        public const String Tabla_His_Nom_Antiguedad_Empleados = "HIS_NOM_ANTIGUEDAD_EMPLEADOS";
        public const String Campo_No_Antiguedad = "NO_ANTIGUEDAD";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Antiguedad = "ANTIGUEDAD";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Fin = "FECHA_FIN";
        public const String Campo_Dias_Tomados = "DIAS_TOMADOS";
        public const String Campo_Dias_Restan = "DIAS_RESTAN";
        public const String Campo_Dias_Tocan = "DIAS_TOCAN";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: His_Nom_Aumento_Sueldo
    /// DESCRIPCION: Clase con contiene los datos de la tabla HIS_NOM_AUMENTO_SUELDO
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class His_Nom_Aumento_Sueldo
    {
        public const String Tabla_His_Nom_Aumento_Sueldo = "HIS_NOM_AUMENTO_SUELDO";
        public const String Campo_No_Historial = "NO_HISTORIAL";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Salario_Diario = "SALARIO_DIARIO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Tab_Nom_Subsidio
    /// DESCRIPCION: Clase con contiene los datos de la tabla TAB_NOM_SUBSIDIO
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Tab_Nom_Subsidio
    {
        public const String Tabla_Tab_Nom_Subsidio = "TAB_NOM_SUBSIDIO";
        public const String Campo_Subsidio_ID = "SUBSIDIO_ID";
        public const String Campo_Limite_Inferior = "LIMITE_INFERIOR";
        public const String Campo_Subsidio = "SUBSIDIO";
        public const String Campo_Tipo_Nomina = "TIPO_NOMINA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Tab_Nom_ISR
    /// DESCRIPCION: Clase con contiene los datos de la tabla TAB_NOM_ISR
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Tab_Nom_ISR
    {
        public const String Tabla_Tab_Nom_ISR = "TAB_NOM_ISR";
        public const String Campo_ISR_ID = "ISR_ID";
        public const String Campo_Limite_Inferior = "LIMITE_INFERIOR";
        public const String Campo_Cuota_Fija = "CUOTA_FIJA";
        public const String Campo_Porcentaje = "PORCENTAJE";
        public const String Campo_Tipo_Nomina = "TIPO_NOMINA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Tab_Nom_INFONAVIT
    /// DESCRIPCION: Clase con contiene los datos de la tabla TAB_NOM_INFONAVIT
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Tab_Nom_INFONAVIT
    {
        public const String Tabla_Tab_Nom_INFONAVIT = "TAB_NOM_INFONAVIT";
        public const String Campo_INFONAVIT_ID = "INFONAVIT_ID";
        public const String Campo_Veces_SMGA = "VECES_SMGA";
        public const String Campo_Porcentaje = "PORCENTAJE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Tab_Nom_IMSS
    /// DESCRIPCION: Clase con contiene los datos de la tabla TAB_NOM_IMSS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Tab_Nom_IMSS
    {
        public const String Tabla_Tab_Nom_IMSS = "TAB_NOM_IMSS";
        public const String Campo_IMSS_ID = "IMSS_ID";
        public const String Campo_Porcentaje_Enf_Mat_Esp = "PORCENTAJE_ENF_MAT_ESP";
        public const String Campo_Porcentaje_Enf_Mat_Pes = "PORCENTAJE_ENF_MAT_PES";
        public const String Campo_Porcentaje_Invalidez_Vida = "PORCENTAJE_INVALIDEZ_VIDA";
        public const String Campo_Porcentaje_Cesantia_Vejez = "PORCENTAJE_CESANTIA_VEJEZ";
        public const String Campo_Excendete_3_SMG_DF = "EXCEDENTE_3_SMG_DF";
        public const String Campo_Prestaciones_Dinero = "PRESTACIONES_DINERO";
        public const String Campo_Gastos_Medicos = "GASTOS_MEDICOS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Tab_Nom_Vacaciones
    /// DESCRIPCION: Clase con contiene los datos de la tabla TAB_NOM_VACACIONES
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Tab_Nom_Vacaciones
    {
        public const String Tabla_Tab_Nom_Vacaciones = "TAB_NOM_VACACIONES";
        public const String Campo_Vacacion_ID = "VACACION_ID";
        public const String Campo_Antiguedad = "ANTIGUEDAD";
        public const String Campo_Dias = "DIAS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Tab_Nom_Dias_Festivos
    /// DESCRIPCION: Clase con contiene los datos de la tabla TAB_NOM_DIAS_FESTIVOS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 19/Agosto/2010
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Tab_Nom_Dias_Festivos
    {
        public const String Tabla_Tab_Nom_Dias_Festivos = "TAB_NOM_DIAS_FESTIVOS";
        public const String Campo_Dia_ID = "DIA_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Nomina_ID = "NOMINA_ID";
    }
    #endregion

    #endregion

    ///*************************************************************************************************************************
    ///                                                                COMPRAS
    ///*************************************************************************************************************************

    #region Compras

    ///*************************************************************************************************************************
    ///                                                                SubRegion Compras
    ///*************************************************************************************************************************
    #region SubRegion_Compras
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Poliza_Stock_Para_Sap
    /// DESCRIPCION:           Clase que contiene los campos de la tabla Poliza_Stock_Para_Sap 
    /// PARAMETROS :     
    /// CREO       :           Gustavo Angeles Cruz
    /// FECHA_CREO :           27/Septiembre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Poliza_Stock_Para_Sap
    {
        public const String Tabla_Poliza_Stock_Para_Sap = "POLIZA_STOCK_PARA_SAP";
        public const String Campo_No_Poliza_Stock = "NO_POLIZA_STOCK";
        public const String Campo_Salidas = "SALIDAS";
        public const String Campo_Hora = "HORA";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Directores
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_DIRECTORES 
    /// PARAMETROS :     
    /// CREO       :           Jesus Toledo Rodriguez
    /// FECHA_CREO :           06/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Directores
    {
        public const String Tabla_Cat_Com_Directores = "CAT_COM_DIRECTORES";
        public const String Campo_Director_Adquisiciones = "DIRECTOR_ADQUISICIONES";
        public const String Campo_Oficial_Mayor = "OFICIAL_MAYOR";
        public const String Campo_Tesorero = "TESORERO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Historial_Req
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla
    /// PARÁMETROS :     
    /// CREO       : Gustavo Angeles Cruz
    /// FECHA_CREO : 01/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Historial_Req
    {
        public const String Campo_No_Historial = "NO_HISTORIAL";
        public const String Campo_No_Requisicion = "NO_REQUISICION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Empleado = "EMPLEADO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Tabla_Ope_Com_Historial_Req = "OPE_COM_HISTORIAL_REQ";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Proveedores
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_PROVEEDORES
    /// PARAMETROS :     
    /// CREO       :           Noe Mosqueda Valadez
    /// FECHA_CREO :           27/Septiembre/2010 12:16 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Proveedores
    {
        public const String Tabla_Cat_Com_Proveedores = "CAT_COM_PROVEEDORES";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Giro_ID = "GIRO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Compañia = "COMPANIA";
        public const String Campo_RFC = "RFC";
        public const String Campo_Contacto = "CONTACTO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Direccion = "DIRECCION";
        public const String Campo_Colonia = "COLONIA";
        public const String Campo_Ciudad = "CIUDAD";
        public const String Campo_Estado = "ESTADO";
        public const String Campo_CP = "CODIGO_POSTAL";
        public const String Campo_Telefono_1 = "TELEFONO1";
        public const String Campo_Telefono_2 = "TELEFONO2";
        public const String Campo_Nextel = "NEXTEL";
        public const String Campo_Fax = "FAX";
        public const String Campo_Correo_Electronico = "E_MAIL";
        public const String Campo_Password = "PASSWORD";
        public const String Campo_Tipo_Pago = "TIPO_PAGO";
        public const String Campo_Dias_Credito = "DIAS_CREDITO";
        public const String Campo_Forma_Pago = "FORMA_PAGO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario = "USUARIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Cuenta = "CUENTA";
        public const String Campo_Actualizacion = "ACTUALIZACION";
        public const String Campo_Fecha_Actualizacion = "FECHA_ACTUALIZACION";
        public const String Campo_Representante_Legal = "REPRESENTANTE_LEGAL";
        public const String Campo_Fecha_Registro = "FECHA_DE_REGISTRO";
        public const String Campo_Tipo_Fiscal = "TIPO_FISCAL";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Rol_ID = "ROL_ID";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Cuenta_Proveedor_ID = "CUENTA_PROVEEDOR_ID";
        public const String Campo_Cuenta_Contratista_ID = "CUENTA_CONTRATISTA_ID";
        public const String Campo_Cuenta_Judicial_ID = "CUENTA_JUDICIAL_ID";
        public const String Campo_Cuenta_Nomina_ID = "CUENTA_NOMINA_ID ";
        public const String Campo_Cuenta_Acreedor_ID = "CUENTA_ACREEDOR_ID ";
        public const String Campo_Cuenta_Predial_ID = "CUENTA_PREDIAL_ID";
        public const String Campo_Cuenta_Deudor_ID = "CUENTA_DEUDOR_ID";
        public const String Campo_Clabe = "CLABE";
        public const String Campo_Banco_Proveedor_ID = "BANCO_PROVEEDOR_ID";
        public const String Campo_Banco_ID = "BANCO_ID";
        public const String Campo_Banco_Proveedor = "BANCO_PROVEEDOR";
        public const String Campo_CURP = "CURP";
        public const String Campo_No_Padron = "NO_PADRON";
        public const String Campo_Provisional = "PROVISIONAL";
        public const String Campo_Cuenta_Anticipo_ID = "Cuenta_Anticipo_ID";
        public const String Campo_Usuario_Mod_Dat_Bancario = "USUARIO_MOD_DAT_BANCARIO";
        public const String Campo_Fecha_Mod_Dat_Bancario = "FECHA_MOD_DAT_BANCARIO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    OPE_COM_HIS_AUTOR_PROV
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_HIS_AUTOR_PROV
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           8/NOV/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Com_His_Autor_Prov
    {
        public const String Tabla_Ope_Com_His_Autor_Prov = "OPE_COM_HIS_AUTOR_PROV";
        public const String Campo_Historial_ID = "HISTORIAL_ID";
        public const String Campo_Fecha_Actualizacion = "FECHA_ACTUALIZACION";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";


    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Com_Det_Part_Prov
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_DET_PART_PROV 
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           8/Septiembre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Det_Part_Prov
    {
        public const String Tabla_Cat_Com_Det_Part_Prov = "CAT_COM_DET_PART_PROV";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Partida_Generica_ID = "PARTIDA_GENERICA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Familias
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_FAMILIAS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 4:47 PM 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Familias
    {
        public const String Tabla_Cat_Com_Familias = "CAT_COM_FAMILIAS";
        public const String Campo_Familia_ID = "FAMILIA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Abreviatura = "ABREVIATURA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Documentos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_DOCUMENTOS
    /// PARAMETROS :     
    /// CREO       :           José Antonio López Hernández
    /// FECHA_CREO :           04/Enero/2011 16:29
    /// MODIFICO          :    Salvador Hernández Ramírez
    /// FECHA_MODIFICO    :    15/Marzo/2011
    /// CAUSA_MODIFICACION:    Se agregó el campo "TIPO"
    ///*******************************************************************************/
    public class Cat_Com_Documentos
    {
        public const String Tabla_Cat_Com_Documentos = "CAT_COM_DOCUMENTOS";
        public const String Campo_Documento_ID = "DOCUMENTO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Tipo = "TIPO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Impuestos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_IMPUESTOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 5:00 PM 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Impuestos
    {
        public const String Tabla_Cat_Impuestos = "CAT_COM_IMPUESTOS";
        public const String Campo_Impuesto_ID = "IMPUESTO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Porcentaje_Impuesto = "PORCENTAJE_IMPUESTO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Marcas_Productos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_MARCAS_PRODUCTOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 05:20 PM
    /// MODIFICO          :     
    /// FECHA_MODIFICO    :     
    /// CAUSA_MODIFICACION:     
    ///*******************************************************************************/
    public class Cat_Com_Marcas_Productos
    {
        public const String Tabla_Cat_Com_Marcas_Productos = "CAT_COM_MARCAS_PRODUCTOS";
        public const String Campo_Marca_ID = "MARCA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Modelos_Productos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_MODELOS_PRODUCTOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 5:35 PM
    /// MODIFICO          :    Salvador Hernández Ramírez
    /// FECHA_MODIFICO    :    18/Diciembre/ 2010
    /// CAUSA_MODIFICACION:    Le agregué las constantes "Campo_Costo" y "Campo_Producto_ID", "Campo_Proveedor_ID" , "Campo_Marca_ID"  y "Ruta_Foto" ya que son necesarias para realizar la consulta a la BD 
    ///*******************************************************************************/
    public class Cat_Com_Modelos_Productos
    {
        public const String Tabla_Cat_Com_Productos = "CAT_COM_PRODUCTOS";
        public const String Campo_Modelo_ID = "MODELO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Costo = "COSTO";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Marca_ID = "MARCA_ID";
        public const String Campo_Ruta_Foto = "RUTA_FOTO";
        public const String Campo_Modelo = "MODELO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Marcas
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_MARCAS_PRODUCTOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 05:20 PM
    /// MODIFICO          :     José Antonio López Hernández
    /// FECHA_MODIFICO    :     07/Enero/2010 16:52
    /// CAUSA_MODIFICACION:     Agregar el campo de Estatus
    ///*******************************************************************************/
    public class Cat_Com_Marcas
    {
        public const String Tabla_Cat_Com_Marcas = "CAT_COM_MARCAS";
        public const String Campo_Marca_ID = "MARCA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Modelos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_MODELOS_PRODUCTOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 5:35 PM
    /// MODIFICO          : Salvador Hernandez Ramirez
    /// FECHA_MODIFICO    : 12/01/2011
    /// CAUSA_MODIFICACION: Se agregaron las constantes "Campo_Estatus y Campo_Subfamilia_ID"
    ///*******************************************************************************/
    public class Cat_Com_Modelos
    {
        public const String Tabla_Cat_Com_Modelos = "CAT_COM_MODELOS";
        public const String Campo_Modelo_ID = "MODELO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Subfamilia_ID = "SUBFAMILIA_ID";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Subfamilia_ID = "SUBFAMILIA_ID";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Productos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_PRODUCTOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 6:12 PM
    /// MODIFICO          :     Jesus Toledo Rdz
    /// FECHA_MODIFICO    :     04-Abril-2011
    /// CAUSA_MODIFICACION:     Modificaciones a la tabla debido a requerimientos
    ///*******************************************************************************/
    public class Cat_Com_Productos
    {
        public const String Tabla_Cat_Com_Productos = "CAT_COM_PRODUCTOS";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Impuesto_ID = "IMPUESTO_ID";
        public const String Campo_Impuesto_2_ID = "IMPUESTO_2_ID";
        public const String Campo_Familia_ID = "FAMILIA_ID";
        public const String Campo_Subfamilia_ID = "SUBFAMILIA_ID";
        public const String Campo_Modelo_ID = "MODELO_ID";
        public const String Campo_Marca_ID = "MARCA_ID";
        public const String Campo_Unidad = "UNIDAD";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Giro_ID = "GIRO_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Stock = "STOCK";
        public const String Campo_Resguardo = "RESGUARDO";
        public const String Campo_Partida_Especifica_ID = "PARTIDA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Costo = "COSTO";
        public const String Campo_Costo_Promedio = "COSTO_PROMEDIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Existencia = "EXISTENCIA";
        public const String Campo_Comprometido = "COMPROMETIDO";
        public const String Campo_Disponible = "DISPONIBLE";
        public const String Campo_Maximo = "MAXIMO";
        public const String Campo_Minimo = "MINIMO";
        public const String Campo_Reorden = "REORDEN";
        public const String Campo_Ubicacion = "UBICACION";
        //public const String Campo_Fecha_Ultimo_Costo = "FECHA_ULTIMO_COSTO";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Ruta_Foto = "RUTA_FOTO";
        public const String Campo_Modelo = "MODELO";
        public const String Campo_Inicial = "INICIAL";
        public const String Campo_Categoria_ID = "CATEGORIA_ID";
        public const String Campo_REF_JAPAMI = "REF_JAPAMI";
        public const String Campo_Almacen_General = "ALMACEN_GENERAL";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Servicios
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_SERVICIOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 7:03 PM
    /// MODIFICO          :    Roberto González Oseguera
    /// FECHA_MODIFICO    :    05/Abril/2011
    /// CAUSA_MODIFICACION:    Se agregó la propiedad Campo_Partida_ID
    ///*******************************************************************************/
    public class Cat_Com_Servicios
    {
        public const String Tabla_Cat_Com_Servicios = "CAT_COM_SERVICIOS";
        public const String Campo_Servicio_ID = "SERVICIO_ID";
        public const String Campo_Impuesto_ID = "IMPUESTO_ID";
        public const String Campo_Giro_ID = "GIRO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Costo = "COSTO";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Tipos_Entradas_Salidas
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_TIPOS_ENT_SAL
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 7:19 PM 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Tipos_Ent_Sal
    {
        public const String Tabla_Cat_Com_Tipos_Ent_Sal = "CAT_COM_TIPOS_ENT_SAL";
        public const String Campo_Tipo_Movimiento_ID = "TIPO_MOVIMIENTO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Abreviatura = "ABREVIATURA";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Unidades
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_UNIDADES_PRODUCTOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 7:30 PM 
    /// MODIFICO          :    Noe Mosqueda Valadez
    /// FECHA_MODIFICO    :    06/Noviembre/2010 13:00
    /// CAUSA_MODIFICACION:    Se cambio el nombre de la tabla de Cat_Com_Unidades_Productos 
    ///                        a Cat_Com_Unidades
    ///*******************************************************************************/
    public class Cat_Com_Unidades
    {
        public const String Tabla_Cat_Com_Unidades = "CAT_COM_UNIDADES";
        public const String Campo_Unidad_ID = "UNIDAD_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Abreviatura = "ABREVIATURA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Giro_Proveedor
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_GIRO_PROVEEDOR
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta 
    /// FECHA_CREO :           29/Octubre/2010 05:00 pm  
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/

    public class Cat_Com_Giro_Proveedor
    {
        public const String Tabla_Cat_Com_Giro_Proveedor = "CAT_COM_GIRO_PROVEEDOR";
        public const String Campo_Giro_ID = "CONCEPTO_ID";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: Cat_Sap_Proyectos_Programas
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla CAT_SAP_PROYECTOS_PROGRAMAS
    ///
    ///CREO: Hugo Enrique Ramirez Aguilera
    ///FECHA CREÓ: 17/Octubra/2011
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Cat_Sap_Proyectos_Programas
    {
        public const String Tabla_Cat_Sap_Proyectos_Programas = "CAT_SAP_PROYECTOS_PROGRAMAS";
        public const String Campo_Proyecto_Programa_Id = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Elemento_Pep = "ELEMENTO_PEP";
        public const String Campo_Nombre = "NOMBRE";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Sap_Det_Prog_Partidas
    /// DESCRIPCION:           
    /// PARAMETROS :     
    /// CREO       :           Gustavo AC
    /// FECHA_CREO :       28 Feb 2011    
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Sap_Det_Prog_Partidas
    {
        public const String Tabla_Cat_Sap_Det_Prog_Partidas = "CAT_SAP_DET_PROG_PARTIDAS";
        public const String Campo_Det_Prog_Partidas_ID = "DET_PROG_PARTIDAS_ID";
        public const String Campo_Det_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Det_Partida_ID = "PARTIDA_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Proyectos_Programas
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_PROYECTOS_PROGRAMAS
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta 
    /// FECHA_CREO :           29/Octubre/2010 05:00 pm  
    /// MODIFICO          :Gustavo AC
    /// FECHA_MODIFICO    :28 Feb 2011
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Proyectos_Programas
    {
        public const String Tabla_Cat_Com_Proyectos_Programas = "CAT_SAP_PROYECTOS_PROGRAMAS";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Nombre = "NOMBRE";//no debe ir
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Elemento_PEP = "ELEMENTO_PEP";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:   Cat_Com_Dep_Presupuesto
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_PROYECTOS_PROGRAMAS
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta 
    /// FECHA_CREO :           29/Octubre/2010 05:00 pm  
    /// MODIFICO          :Gustavo AC
    /// FECHA_MODIFICO    :28 Feb 2011
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Dep_Presupuesto
    {
        public const String Tabla_Cat_Com_Dep_Presupuesto = "OPE_SAP_DEP_PRESUPUESTO";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Anio_Presupuesto = "ANIO_PRESUPUESTO";
        public const String Campo_Monto_Presupuestal = "MONTO_PRESUPUESTAL";
        public const String Campo_Monto_Comprometido = "MONTO_COMPROMETIDO";
        public const String Campo_Monto_Disponible = "MONTO_DISPONIBLE";
        public const String Campo_Monto_Ejercido = "MONTO_EJERCIDO";
        public const String Campo_No_Asignacion_Anio = "NO_ASIGNACION_ANIO";
        public const String Campo_Fecha_Asignacion = "FECHA_ASIGNACION";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Presupuesto_ID = "PRESUPUESTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        //nuevos Campos
        public const String Campo_Monto_Ampliacion = "MONTO_AMPLIACION";
        public const String Campo_Monto_Reduccion = "MONTO_REDUCCION";
        public const String Campo_Monto_Modificado = "MONTO_MODIFICADO";
        public const String Campo_Monto_Devengado = "MONTO_DEVENGADO";
        public const String Campo_Monto_Pagado = "MONTO_PAGADO";
        public const String Campo_Monto_Devengado_Pagado = "MONTO_DEVEGANDO_PAGADO";
        public const String Campo_Monto_Comprometido_Real = "MONTO_COMPROMETIDO_REAL";
        public const String Campo_No_Presupuesto = "NO_PRESUPUESTO";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:   Ope_Com_Pres_Prog_Proy
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_PROYECTOS_PROGRAMAS
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta 
    /// FECHA_CREO :           29/Octubre/2010 05:00 pm  
    /// MODIFICO          :Gustavo AC
    /// FECHA_MODIFICO    :28 feb 2011
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/

    public class Ope_Com_Pres_Prog_Proy
    {
        public const String Tabla_Ope_Com_Pres_Prog_Proy = "OPE_SAP_PRES_PROG_PROY";
        public const String Campo_Pres_Prog_Proy_ID = "PRES_PROG_PROY_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Tipo_Presupuesto = "TIPO_PRESUPUESTO";
        public const String Campo_Monto_Presupuestal = "MONTO_PRESUPUESTAL";
        public const String Campo_Monto_Comprometido = "MONTO_COMPROMETIDO";
        public const String Campo_Monto_Disponible = "MONTO_DISPONIBLE";
        public const String Campo_Monto_Ejercido = "MONTO_EJERCIDO";
        public const String Campo_Anio_Presupuesto = "ANIO_PRESUPUESTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Com_Partidas
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_COM_PARTIDAS
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 29/Octubre/2010 
    /// MODIFICO          :Gustavo Angeles
    /// FECHA_MODIFICO    :28 Feb 2011
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Com_Partidas
    {
        public const String Tabla_Cat_Com_Partidas = "CAT_SAP_PARTIDAS_ESPECIFICAS";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_PrOyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";//no debe ir
        public const String Campo_Giro_ID = "GIRO_ID";
        public const String Campo_Presupuesto_Prog_Poy_ID = "PRES_PROG_PROY_ID";//no debe ir
        public const String Campo_Nombre = "NOMBRE";//No debe ir
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Partida_Generica_ID = "PARTIDA_GENERICA_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Operacion = "OPERACION";
        public const String Campo_Cuenta_SAP = "CUENTA";
        public const String Campo_Clave_SAP = "CLAVE_SAP";
        public const String Campo_Centro_Aplicacion = "CENTRO_APLICACION";
        public const String Campo_Afecta_Area_Funcional = "AFECTA_AREA_FUNCIONAL";
        public const String Campo_Afecta_Partida = "AFECTA_PARTIDA";
        public const String Campo_Afecta_Elemento_PEP = "AFECTA_ELEMENTO_PEP";
        public const String Campo_Afecta_Fondo = "AFECTA_FONDO";
        public const String Campo_Descripcion_Especifica = "DESCRIPCION_ESPECIFICA";
    }



    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Com_Subfamilias
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_COM_SUBFAMILIAS
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 9/nOVIEMBRE/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Com_Subfamilias
    {
        public const String Campo_Subfamilia_ID = "SUBFAMILIA_ID";
        public const String Campo_Familia_ID = "FAMILIA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Abreviatura = "ABREVIATURA";
        public const String Campo_Descripcion = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Tabla_Cat_Com_Subfamilias = "CAT_COM_SUBFAMILIAS";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Pres_Partida
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_COM_PARTIDAS
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 29/Octubre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Pres_Partida
    {
        public const String Campo_Presupuesto_Partida_ID = "PRESUPUESTO_PARTIDA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Monto_Presupuestal = "MONTO_PRESUPUESTAL";
        public const String Campo_Monto_Comprometido = "MONTO_COMPROMETIDO";
        public const String Campo_Monto_Disponible = "MONTO_DISPONIBLE";
        public const String Campo_Monto_Ejercido = "MONTO_EJERCIDO";
        public const String Campo_Anio_Presupuesto = "ANIO_PRESUPUESTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Tabla_Ope_Com_Pres_Partida = "OPE_SAP_PRES_PARTIDAS";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Req_Producto
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_COM_REQ_PRODUCTO
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 01/Noviembre/2010 
    /// MODIFICO          :Susana Trigueros Armenta
    /// FECHA_MODIFICO    :24/Enero/2011
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Req_Producto
    {
        public const String Campo_Ope_Com_Req_Producto_ID = "OPE_COM_REQ_PRODUCTO_ID";
        public const String Campo_Requisicion_ID = "NO_REQUISICION";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Servicio_ID = "SERVICIO_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Importe = "MONTO";
        public const String Campo_Monto_IVA = "MONTO_IVA";
        public const String Campo_Monto_IEPS = "MONTO_IEPS";
        public const String Campo_Monto_Total = "MONTO_TOTAL";
        public const String Campo_Porcentaje_IVA = "PORCENTAJE_IVA";
        public const String Campo_Porcentaje_IEPS = "PORCENTAJE_IEPS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Tabla_Ope_Com_Req_Producto = "OPE_COM_REQ_PRODUCTO";
        public const String Campo_Precio_Unitario = "PRECIO_UNITARIO";
        //Campos nuevos 
        public const String Campo_Prod_Serv_ID = "PROD_SERV_ID";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Giro_ID = "CONCEPTO_ID";
        public const String Campo_Nombre_Giro = "NOMBRE_CONCEPTO";
        public const String Campo_Nombre_Producto_Servicio = "NOMBRE_PRODUCTO_SERVICIO";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Nombre_Proveedor = "NOMBRE_PROVEEDOR";
        public const String Campo_Precio_U_Sin_Imp_Cotizado = "PRECIO_U_SIN_IMP_COTIZADO";
        public const String Campo_Subtota_Cotizado = "SUBTOTAL_COTIZADO";
        public const String Campo_IVA_Cotizado = "IVA_COTIZADO";
        public const String Campo_IEPS_Cotizado = "IEPS_COTIZADO";
        public const String Campo_Total_Cotizado = "TOTAL_COTIZADO";
        public const String Campo_Empleado_Cotizador_ID = "EMPLEADO_COTIZADOR_ID";
        public const String Campo_Precio_U_Con_Imp_Cotizado = "PRECIO_U_CON_IMP_COTIZADO";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_No_Orden_Compra = "NO_ORDEN_COMPRA";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Resguardado = "RESGUARDADO";
        public const String Campo_Cantidad_Entregada = "CANTIDAD_ENTREGADA";

    }
    /////*******************************************************************************
    ///// NOMBRE DE LA CLASE: Ope_Com_Req_Producto
    ///// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_COM_REQ_PRODUCTO
    ///// PARÁMETROS :     
    ///// CREO       : Silvia Morales Portuhondo
    ///// FECHA_CREO : 01/Noviembre/2010 
    ///// MODIFICO          :
    ///// FECHA_MODIFICO    :
    ///// CAUSA_MODIFICACIÓN:
    /////*******************************************************************************
    //public class Ope_Com_Req_Producto
    //{
    //    public const String Campo_Ope_Com_Req_Producto_ID = "OPE_COM_REQ_PRODUCTO_ID";
    //    public const String Campo_Requisicion_ID = "NO_REQUISICION";
    //    public const String Campo_Producto_ID = "PRODUCTO_ID";
    //    public const String Campo_Servicio_ID = "SERVICIO_ID";
    //    public const String Campo_Partida_ID = "PARTIDA_ID";
    //    public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
    //    public const String Campo_Cantidad = "CANTIDAD";
    //    public const String Campo_Importe = "MONTO";
    //    public const String Campo_Monto_IVA = "MONTO_IVA";
    //    public const String Campo_Monto_IEPS = "MONTO_IEPS";
    //    public const String Campo_Monto_Total = "MONTO_TOTAL";
    //    public const String Campo_Porcentaje_IVA = "PORCENTAJE_IVA";
    //    public const String Campo_Porcentaje_IEPS = "PORCENTAJE_IEPS";
    //    public const String Campo_Usuario_Creo = "USUARIO_CREO";
    //    public const String Campo_Fecha_Creo = "FECHA_CREO";
    //    public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
    //    public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    //    public const String Tabla_Ope_Com_Req_Producto = "OPE_COM_REQ_PRODUCTO";
    //}

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Documentos_Requisicion
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_DOCUMENTOS_REQUISICION
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 01/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Documentos_Requisicion
    {
        public const String Campo_Documento_ID = "DOCUMENTO_ID";
        public const String Campo_Requisicion_ID = "NO_REQUISICION";
        public const String Campo_Url = "URL";
        public const String Campo_Nombre_Documento = "NOMBRE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Tabla_Ope_Documentos_Requisicion = "OPE_DOCUMENTOS_REQUISICION";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Dep_Partidas
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla
    /// PARÁMETROS :     
    /// CREO       : Silvia Morales Portuhondo
    /// FECHA_CREO : 01/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Dep_Partidas
    {
        public const String Campo_Dependencia_Partida_ID = "DEPENDENCIA_PARTIDA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Monto_Presupuestal = "MONTO_PRESUPUESTAL";
        public const String Campo_Monto_Comprometido = "MONTO_COMPROMETIDO";
        public const String Campo_Monto_Disponible = "MONTO_DISPONIBLE";
        public const String Campo_Anio_Presupuesto = "ANIO_PRESUPUESTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Tabla_Ope_Com_Dep_Partidas = "OPE_COM_DEP_PARTIDAS";
    }
    /////*******************************************************************************
    ///// NOMBRE DE LA CLASE: Ope_Com_Propuesta_Cotizacion
    ///// DESCRIPCIÓN: Clase que contiene los campos de la tabla Ope_Com_Propuesta_Cotizacion
    ///// PARÁMETROS :     
    ///// CREO       : Susana Trigueros Armenta 
    ///// FECHA_CREO : 03/Julio/2010 
    ///// MODIFICO          :
    ///// FECHA_MODIFICO    :
    ///// CAUSA_MODIFICACIÓN:
    /////*******************************************************************************
    public class Ope_Com_Propuesta_Cotizacion
    {
        public const String Tabla_Ope_Com_Propuesta_Cotizacion = "OPE_COM_PROPUESTA_COTIZACION";
        public const String Campo_No_Propuesta_Cotizacion = "NO_PROPUESTA_COTIZACION";
        public const String Campo_Ope_Com_Req_Producto_ID = "OPE_COM_REQ_PRODUCTO_ID";
        public const String Campo_No_Requisicion = "NO_REQUISICION";

        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Prod_Serv_ID = "PROD_SERV_ID";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Nombre_Producto_Servicion = "NOMBRE_PRODUCTO_SERVICIO";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";

        public const String Campo_Precio_U_Sin_Imp_Cotizado = "PRECIO_U_SIN_IMP_COTIZADO";
        public const String Campo_Subtota_Cotizado = "SUBTOTAL_COTIZADO";
        public const String Campo_IVA_Cotizado = "IVA_COTIZADO";
        public const String Campo_IEPS_Cotizado = "IEPS_COTIZADO";
        public const String Campo_Total_Cotizado = "TOTAL_COTIZADO";
        public const String Campo_Precio_U_Con_Imp_Cotizado = "PRECIO_U_CON_IMP_COTIZADO";
        public const String Campo_Nombre_Cotizador = "NOMBRE_COTIZADOR";

        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

        public const String Campo_IEPS_Cotizado_Req = "IEPS_COTIZADO_REQ";
        public const String Campo_IVA_Cotizado_Req = "IVA_COTIZADO_REQ";
        public const String Campo_Total_Cotizado_Requisicion = "TOTAL_COTIZADO_REQUISICION";
        public const String Campo_Subtotal_Cotizado_Requisicion = "SUBTOTAL_COTIZADO_REQUISICION";

        public const String Campo_Fecha_Elaboracion = "FECHA_ELABORACION";
        public const String Campo_Tiempo_Entrega = "TIEMPO_ENTREGA";
        public const String Campo_Garantia = "GARANTIA";
        public const String Campo_Vigencia_Propuesta = "VIGENCIA_PROPUESTA";
        public const String Campo_Registro_Padron_Prov = "REGISTRO_PADRON_PROV";
        public const String Campo_Descripcion_Producto = "DESCRIPCION_PRODUCTO";
        public const String Campo_Marca = "MARCA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Resultado = "RESULTADO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Requisiciones
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla Ope_Com_Requisiciones
    /// PARÁMETROS :     
    /// CREO       : Susana Trigueros Armenta 
    /// FECHA_CREO : 01/Noviembre/2010 
    /// MODIFICO          :Gustavo Angeles Cruz
    /// FECHA_MODIFICO    :21 ENE 2011
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************

    public class Ope_Com_Requisiciones
    {
        public const String Tabla_Ope_Com_Requisiciones = "OPE_COM_REQUISICIONES";
        public const String Campo_Requisicion_ID = "NO_REQUISICION";
        public const String Campo_Area_ID = "AREA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Codigo_Programatico = "CODIGO_PROGRAMATICO";
        public const String Campo_Subtotal = "SUBTOTAL";
        public const String Campo_IVA = "IVA";
        public const String Campo_IEPS = "IEPS";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Total = "TOTAL";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Fecha_Construccion = "FECHA_CONSTRUCCION";
        public const String Campo_Empleado_Construccion_ID = "EMPLEADO_CONSTRUCCION_ID";
        public const String Campo_Fecha_Generacion = "FECHA_GENERACION";
        public const String Campo_Empleado_Generacion_ID = "EMPLEADO_GENERACION_ID";
        public const String Campo_Fecha_Autorizacion = "FECHA_AUTORIZACION";
        public const String Campo_Empleado_Autorizacion_ID = "EMPLEADO_AUTORIZACION_ID";
        public const String Campo_Fecha_Surtido = "FECHA_SURTIDO";
        public const String Campo_Empleado_Surtido_ID = "EMPLEADO_SURTIDO_ID";
        public const String Campo_Fecha_Filtrado = "FECHA_FILTRADO";
        public const String Campo_Empleado_Filtrado_ID = "EMPLEADO_FILTRADO_ID";
        public const String Campo_Fecha_Distribucion = "FECHA_DISTRIBUCION";
        public const String Campo_Empleado_Distribucion_ID = "EMPLEADO_DISTRIBUCION_ID";
        public const String Campo_Fecha_Cotizacion = "FECHA_COTIZACION";
        public const String Campo_Empleado_Cotizacion_ID = "EMPLEADO_COTIZACION_ID";
        public const String Campo_Fecha_Confirmacion = "FECHA_CONFIRMACION";
        public const String Campo_Empleado_Confirmacion_ID = "EMPLEADO_CONFIRMACION_ID";
        public const String Campo_Fase = "FASE";
        public const String Campo_Justificacion_Compra = "JUSTIFICACION_COMPRA";
        public const String Campo_Especificacion_Prod_Serv = "ESPECIFICACION_PROD_SERV";
        public const String Campo_Verificaion_Entrega = "VERIFICACION_ENTREGA";
        public const String Campo_No_Consolidacion = "NO_CONSOLIDACION";
        public const String Campo_Consolidada = "CONSOLIDADA";
        public const String Campo_Tipo_Compra = "TIPO_COMPRA";
        public const String Campo_Fecha_Cancelada = "FECHA_CANCELADA";
        public const String Campo_Empleado_Cancelada_ID = "EMPLEADO_CANCELADA_ID";
        public const String Campo_Fecha_Rechazo = "FECHA_RECHAZADA";
        public const String Campo_Empleado_Rechazo_ID = "EMPLEADO_RECHAZO_ID";
        public const String Campo_Fecha_Cotizada_Rechazada = "FECHA_COTIZADA_RECHAZADA";
        public const String Campo_Empleado_Cotizada_Rechazada_ID = "EMPLEADO_COTIZA_RECHAZA_ID";
        public const String Campo_No_Cotizacion = "NO_COTIZACION";
        public const String Campo_No_Comite_Compras = "NO_COMITE_COMPRAS";
        public const String Campo_No_Licitacion = "NO_LICITACION";
        public const String Campo_No_Orden_Compra = "NO_ORDEN_COMPRA";
        public const String Campo_Subtotal_Cotizado = "SUBTOTAL_COTIZADO";
        public const String Campo_IEPS_Cotizado = "IEPS_COTIZADO";
        public const String Campo_IVA_Cotizado = "IVA_COTIZADO";
        public const String Campo_Total_Cotizado = "TOTAL_COTIZADO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Tipo_Articulo = "TIPO_ARTICULO";
        public const String Campo_Precio_Unitario = "PRECIO_UNITARIO";
        public const String Campo_Listado_Almacen = "LISTADO_ALMACEN";
        public const String Campo_Cotizador_ID = "COTIZADOR_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Elemento_PEP = "ELEMENTO_PEP";
        public const String Campo_Alerta = "ALERTA";
    }


    //}
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Cat_Com_Giros
    /// DESCRIPCION:            Clase que contiene los campos de la tabla CAT_COM_GIROS
    /// PARAMETROS :     
    /// CREO       :            Noe Mosqueda Valadez
    /// FECHA_CREO :            03/Noviembre/2010 18:48 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Com_Giros
    {
        public const String Tabla_Cat_Com_Giros = "CAT_COM_GIROS";
        public const String Campo_Giro_ID = "GIRO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Req_Observaciones
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_COM_REQ_OBSERVACIONES
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            08/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************

    public class Ope_Com_Req_Observaciones
    {
        public const String Tabla_Ope_Com_Req_Observaciones = "OPE_COM_REQ_OBSERVACIONES";
        public const String Campo_Observacion_ID = "OBSERVACION_ID";
        public const String Campo_Requisicion_ID = "NO_REQUISICION";
        public const String Campo_Comentario = "COMENTARIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:  Cat_Com_Monto_Proceso_Compra
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla  Cat_Com_Monto_Proceso_Compra
    /// PARÁMETROS :     
    /// CREO       : Susana Trigueros Armenta 
    /// FECHA_CREO : 01/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Com_Monto_Proceso_Compra
    {
        public const String Tabla_Cat_Com_Monto_Proceso_Compra = "CAT_COM_MONTO_PROCESO_COMPRA";
        public const String Campo_Parametro_ID = "PARAMETRO_ID";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Monto_Compra_Directa_Ini = "MONTO_COMPRA_DIRECTA_INI";
        public const String Campo_Monto_Compra_Directa_Fin = "MONTO_COMPRA_DIRECTA_FIN";
        public const String Campo_Monto_Cotizacion_Ini = "MONTO_COTIZACION_INI";
        public const String Campo_Monto_Cotizacion_Fin = "MONTO_COTIZACION_FIN";
        public const String Campo_Monto_Comite_Ini = "MONTO_COMITE_INI";
        public const String Campo_Monto_Comite_Fin = "MONTO_COMITE_FIN";
        public const String Campo_Monto_Licitacion_R_Ini = "MONTO_LICITACION_R_INI";
        public const String Campo_Monto_Licitacion_R_Fin = "MONTO_LICITACION_R_FIN";
        public const String Campo_Monto_Licitacion_P_Ini = "MONTO_LICITACION_P_INI";
        public const String Campo_Monto_Licitacion_P_Fin = "MONTO_LICITACION_P_FIN";
        public const String Campo_Fondo_Fijo_Ini = "FONDO_FIJO_INI";
        public const String Campo_Fondo_Fijo_Fin = "FONDO_FIJO__FIN";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Ordenes_Compra
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla Ope_Com_Ordenes_Compra
    /// PARÁMETROS :     
    /// CREO       :            Noe Mosqueda Valadez 
    /// FECHA_CREO :            26/Noviembre/2010 12:09 
    /// MODIFICO           :Gustavo Angeles Cruz
    /// FECHA_MODIFICO     :18 Ene 2011
    /// CAUSA_MODIFICACIÓN :Se agregaron campos
    /// MODIFICO           :Salvador Hernández Ramírez
    /// FECHA_MODIFICO     :10/Febrero/2011
    /// CAUSA_MODIFICACIÓN :Se agregaron las constantes "Campo_Resguardada", "Campo_Usuario_Id_Resguardo" y "Campo_Recibo_Transitorio"
    ///*******************************************************************************
    public class Ope_Com_Ordenes_Compra
    {
        public const String Tabla_Ope_Com_Ordenes_Compra = "OPE_COM_ORDENES_COMPRA";
        public const String Campo_No_Orden_Compra = "NO_ORDEN_COMPRA";
        public const String Campo_No_Requisicion = "NO_REQUISICION";
        public const String Campo_No_Licitacion = "NO_LICITACION";
        public const String Campo_Tipo_Proceso = "TIPO_PROCESO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Nombre_Proveedor = "NOMBRE_PROVEEDOR";
        public const String Campo_Subtotal = "SUBTOTAL";
        public const String Campo_Total_IEPS = "TOTAL_IEPS";
        public const String Campo_Total_IVA = "TOTAL_IVA";
        public const String Campo_Total = "TOTAL";
        public const String Campo_Fecha_Ejercio = "FECHA_EJERCIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Factura_Interno = "NO_CONTRA_RECIBO";
        public const String Campo_No_Cotizacion = "NO_COTIZACION";
        public const String Campo_Resguardada = "RESGUARDADA";
        public const String Campo_Usuario_Id_Resguardo = "USUARIO_ID_RESGUARDO";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Lista_Requisiciones = "LISTA_REQUISICIONES";
        public const String Campo_Tipo_Articulo = "TIPO_ARTICULO";
        public const String Campo_No_Comite_Compras = "NO_COMITE_COMPRAS";
        public const String Campo_Fecha_Entrega = "FECHA_ENTREGA";
        public const String Campo_Recibo_Transitorio = "RECIBO_TRANSITORIO";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_Registrada = "REGISTRADA";
        public const String Campo_Fecha_Cancelacion = "FECHA_CANCELACION";
        public const String Campo_Empleado_ID_Cancelacion = "EMPLEADO_ID_CANCELACION";
        public const String Campo_Motivo_Cancelacion = "MOTIVO_CANCELACION";
        public const String Campo_Impresa = "IMPRESA";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Detalle_Orden_Compra
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla 
    /// Ope_Com_Detalle_Orden_Compra
    /// PARÁMETROS :     
    /// CREO       :            Gustavo Angeles Cruz
    /// FECHA_CREO :            18/Enero/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Detalle_Orden_Compra
    {
        public const String Tabla_Ope_Com_Ordenes_Compra = "OPE_COM_DETALLE_ORDEN_COMPRA";
        public const String Campo_No_Detalle = "NO_DETALLE";
        public const String Campo_No_Orden_Compra = "NO_ORDEN_COMPRA";
        public const String Campo_Prod_Serv_ID = "PROD_SERV_ID";
        public const String Campo_Nombre_Prod_Serv = "NOMBRE_PROD_SERV";
        public const String Campo_Precio_Unitario = "PRECIO_UNITARIO";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Subtotal = "SUBTOTAL";
        public const String Campo_Total_IEPS = "TOTAL_IEPS";
        public const String Campo_Total_IVA = "TOTAL_IVA";
        public const String Campo_Porcentaje_IEPS = "PORCENTAJE_IEPS";
        public const String Campo_Porcentaje_IVA = "PORCENTAJE_IVA";
        public const String Campo_Total = "TOTAL";
        public const String Campo_Giro_ID = "GIRO_ID";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Coment_orden_Comp
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla 
    /// Ope_Com_Coment_orden_Comp
    /// PARÁMETROS :     
    /// CREO       :            Gustavo Angeles Cruz
    /// FECHA_CREO :            18/Enero/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Coment_orden_Comp
    {
        public const String Tabla_Ope_Com_Ordenes_Compra = "OPE_COM_COMENT_ORDEN_COMP";
        public const String Campo_No_Comentario = "NO_COMENTARIO";
        public const String Campo_No_Orden_Compra = "NO_ORDEN_COMPRA";
        public const String Campo_Comentario = "COMENTARIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Cotizaciones
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla Ope_Com_Cotizaciones
    /// PARÁMETROS :     
    /// CREO       :            Noe Mosqueda Valadez 
    /// FECHA_CREO :            26/Noviembre/2010 12:22 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Cotizaciones
    {
        public const String Tabla_Ope_Com_Cotizaciones = "OPE_COM_COTIZACIONES";
        public const String Campo_No_Cotizacion = "NO_COTIZACION";
        public const String Campo_Empleado_Cotizador_ID = "EMPLEADO_COTIZADOR_ID";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Fecha_Precotizacion = "FECHA_PRECOTIZACION";
        public const String Campo_Fecha_Cotizacion = "FECHA_COTIZACION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Condiciones = "CONDICIONES";
        public const String Campo_Tiempo_Entrega = "TIEMPO_ENTREGA";
        public const String Campo_SubTotal_Con_Impuesto = "SUBTOTAL_CON_IMPUESTO";
        public const String Campo_SubTotal_Sin_Impuesto = "SUBTOTAL_SIN_IMPUESTO";
        public const String Campo_IVA = "IVA";
        public const String Campo_IEPS = "IEPS";
        public const String Campo_Total = "TOTAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Lista_Requisiciones = "LISTA_REQUISICIONES";
        public const String Campo_Num_Proveedores = "NUM_PROVEEDORES";
        public const String Campo_Total_Cotizado = "TOTAL_COTIZADO";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Listado_Almacen = "LISTADO_ALMACEN";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Det_Cotizaciones
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla OPE_COM_DET_COTIZACIONES
    /// PARÁMETROS :     
    /// CREO       :            Noe Mosqueda Valadez 
    /// FECHA_CREO :            26/Noviembre/2010 12:22 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Det_Cotizaciones
    {
        public const String Tabla_Ope_Com_Det_Cotizaciones = "OPE_COM_DET_COTIZACIONES";
        public const String Campo_No_Cotizacion = "NO_COTIZACION";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Giro_ID = "CONCEPTO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Cot_Detalles
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla Ope_Com_Cot_Detalles
    /// PARÁMETROS :     
    /// CREO       :            Noe Mosqueda Valadez 
    /// FECHA_CREO :            26/Noviembre/2010 12:27 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Cot_Detalles
    {
        public const String Tabla_Ope_Com_Cot_Detalles = "OPE_COM_COT_DETALLES";
        public const String Campo_No_Cotizacion = "NO_COTIZACION";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Servicio_ID = "SERVICIO_ID";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Costo_Real = "COSTO_REAL";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Ultimo_Costo = "ULTIMO_COSTO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Consolidacion_Req
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla Ope_Com_Consolidacion_Req
    /// PARÁMETROS :     
    /// CREO       :            Noe Mosqueda Valadez 
    /// FECHA_CREO :            26/Noviembre/2010 12:32 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Consolidacion_Req
    {
        public const String Tabla_Ope_Com_Consolidacion_Req = "OPE_COM_CONSOLIDACION_REQ";
        public const String Campo_No_Cotizacion = "NO_COTIZACION";
        public const String Campo_No_Requisicion = "NO_REQUISICION";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Servicio_ID = "SERVICIO_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Familias_Productos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_FAMILIAS_PRODUCTOS
    /// PARAMETROS :     
    /// CREO       :           Luz Verónica Gómez García
    /// FECHA_CREO :           27/Septiembre/2010 4:47 PM 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Familias_Productos
    {
        public const String Tabla_Cat_Com_Familias_Productos = "CAT_COM_FAMILIAS_PRODUCTOS";
        public const String Campo_Familia_ID = "FAMILIA_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Parametros
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_PARAMETROS
    /// PARAMETROS :     
    /// CREO       :           Noe Mosqueda Valadez
    /// FECHA_CREO :           24/Diciembre/2010 14:54 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Parametros
    {
        public const String Tabla_Cat_Com_Parametros = "CAT_COM_PARAMETROS";
        public const String Campo_Parametro_ID = "PARAMETRO_ID";
        public const String Campo_Cantidad_Sal_Min_Resguardo = "CANTIDAD_SAL_MIN_RESGUARDO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Plazo_Surtir_Orden_Compra = "PLAZO_SURTIR_ORDEN_COMPRA";
        public const String Campo_Partida_Gen_Almacen_Global = "PARTIDA_GEN_ALMACEN_GLOBAL";
        public const String Campo_Partida_Esp_Almacen_Global = "PARTIDA_ESP_ALMACEN_GLOBAL";
        public const String Campo_Dependencia_ID_Almacen = "DEPENDENCIA_ID_ALMACEN";
        public const String Campo_Programa_Almacen = "PROGRAMA_ALMACEN";
        public const String Campo_Rol_Proveedor_ID = "ROL_PROVEEDOR_ID";
        public const String Campo_Monto_Compra = "MONTO_COMPRA";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Licitaciones
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_COM_LICITACIONES
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            27/Diciembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Com_Licitaciones
    {
        public const String Tabla_Ope_Com_Licitaciones = "OPE_COM_LICITACIONES";
        public const String Campo_No_Licitacion = "NO_LICITACION";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Fin = "FECHA_FIN";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Monto_Total = "MONTO_TOTAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Lista_Requisiciones = "LISTA_REQUISICIONES";
        public const String Campo_Clasificacion = "CLASIFICACION";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Total_Cotizado = "TOTAL_COTIZADO";

    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Licitacion_Detalles
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_COM_LICITACION_DETALLES
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            27/Diciembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Com_Licitacion_Detalle
    {
        public const String Tabla_Ope_Com_Licitacion_Detalle = "OPE_COM_LICITACION_DETALLE";
        public const String Campo_No_Licitacion = "NO_LICITACION";
        public const String Campo_No_Requisicion = "NO_REQUISICION";
        public const String Campo_Ope_Com_Req_Producto_ID = "OPE_COM_REQ_PRODUCTO_ID";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Monto_Detalle = "MONTO_DETALLE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Licitacion_Proveedor
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_COM_LICITACION_PROVEEDOR
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            3/Ene/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Com_Licitacion_Proveedor
    {
        public const String Tabla_Ope_Com_Licitacion_Proveedor = "OPE_COM_LICITACION_PROVEEDOR";
        public const String Campo_No_Licitacion_Proveedor = "NO_LICITACION_PROVEEDOR";
        public const String Campo_No_Licitacion = "NO_LICITACION";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Licitacion_Proveedor
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_COM_LICITACION_PROVEEDOR
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :             3/Ene/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Com_Licitacion_Partidas
    {
        public const String Tabla_Ope_Com_Licitacion_Partidas = "OPE_COM_LICITACION_PARTIDAS";
        public const String No_Licitacion_Proveedor = "NO_LICITACION_PROVEEDOR";
        public const String Partida_ID = "PARTIDA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Consolidaciones
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_COM_CONSOLIDACIONES
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            28/Diciembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Com_Consolidaciones
    {
        public const String Tabla_Ope_Com_Consolidaciones = "OPE_COM_CONSOLIDACIONES";
        public const String Campo_No_Consolidacion = "NO_CONSOLIDACION";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Monto = "MONTO";
        public const String Campo_Lista_Requisiciones = "LISTA_REQUISICIONES";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Tipo = "TIPO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Det_Consolidacion
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_COM_DET_CONSOLIDACION
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            28/Diciembre/2010 
    /// MODIFICO          :    Gustavo Angeles Cruz
    /// FECHA_MODIFICO    :    9 ene 2011
    /// CAUSA_MODIFICACION:    Cambiaron campos de la Base de Datos
    ///*******************************************************************************
    public class Ope_Com_Det_Consolidacion
    {
        public const String Tabla_Ope_Com_Det_Consolidacion = "OPE_COM_DET_CONSOLIDACION";
        public const String Campo_No_Detalle_Consolidacion = "NO_DETALLE_CONSOLIDACION";
        public const String Campo_No_Consolidacion = "NO_CONSOLIDACION";
        public const String Campo_Producto_Servicio_ID = "PRODUCTO_SERVICIO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Giro_ID = "GIRO_ID";
        public const String Campo_Nombre_Giro = "NOMBRE_GIRO";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Monto_Con_Impuesto = "MONTO_CON_IMPUESTO";
        public const String Campo_Clave_Prod_Serv = "CLAVE_PROD_SERV";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Com_Comite_Compras
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_COMITE_COMPRAS 
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           19/Enero/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Com_Comite_Compras
    {
        public const String Tabla_Ope_Com_Comite_Compras = "OPE_COM_COMITE_COMPRAS";
        public const String Campo_No_Comite_Compras = "NO_COMITE_COMPRAS";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Monto_Total = "MONTO_TOTAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Lista_Requisiciones = "LISTA_REQUISICIONES";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Total_Cotizado = "TOTAL_COTIZADO";


    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Cotizadores
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_COTIZADORES 
    /// PARAMETROS :     
    /// CREO       :           Jacqueline Ramirez Sierra
    /// FECHA_CREO :           05/Febrero/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Cotizadores
    {
        public const String Tabla_Cat_Com_Cotizadores = "CAT_COM_COTIZADORES";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Nombre_Completo = "NOMBRE_COMPLETO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Correo = "CORREO";
        public const String Campo_Password_Correo = "PASSWORD_CORREO";
        public const String Campo_IP_Correo_Saliente = "IP_CORREO_SALIENTE";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Com_Det_Cotizadores
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_DET_COTIZADORES 
    /// PARAMETROS :     
    /// CREO       :           Jacqueline Ramirez Sierra
    /// FECHA_CREO :           11/Febrero/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Det_Cotizadores
    {
        public const String Tabla_Cat_Com_Det_Cotizadores = "CAT_COM_DET_COTIZADORES";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Giro_ID = "CONCEPTO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    #endregion Fin_SubRegion_Compras

    ///*************************************************************************************************************************
    ///                                                                SubRegion Almacen
    ///*************************************************************************************************************************
    #region SubRegion_Almacen
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:   Ope_Alm_Ajustes_Inv_Stock
    /// DESCRIPCION:          Clase de constantes de la tabla Ope_Alm_Ajustes_Inv_Stock
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           30/Septiembre/2011 6:48 PM
    /// MODIFICO          :    
    /// FECHA_MODIFICO    :    
    ///*******************************************************************************/
    public class Ope_Alm_Ajustes_Inv_Stock
    {
        public const String Campo_No_Ajuste = "NO_AJUSTE";
        public const String Campo_Fecha_Hora = "FECHA_HORA";
        public const String Campo_Motivo_Ajuste_Coor = "MOTIVO_AJUSTE_COOR";
        public const String Campo_Motivo_Ajuste_Dir = "MOTIVO_AJUSTE_DIR";
        public const String Campo_Empleado_Elaboro_ID = "EMPLEADO_ELABORO_ID";
        public const String Campo_Empleado_Autorizo_ID = "EMPLEADO_AUTORIZO_ID";
        public const String Campo_Empleado_Rechazo_ID = "EMPLEADO_RECHAZO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Fecha_Elaboro = "FECHA_ELABORO";
        public const String Campo_Fecha_Autorizo = "FECHA_AUTORIZO";
        public const String Campo_Fecha_Rechazo = "FECHA_RECHAZO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Tabla_Ope_Alm_Ajustes_Inv_Stock = "OPE_ALM_AJUSTES_INV_STOCK";
    }


    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Ope_Alm_Ajustes_Detalles
    ///DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_ALM_AJUSTES_DETALLES
    ///PARAMETROS: 
    ///CREO: Francisco Antonio Gallardo Castañeda
    ///FECHA_CREO: 30/Septiembre/2011 06:47:25 p.m.
    ///MODIFICO: 
    ///FECHA_MODIFICO:  
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Alm_Ajustes_Detalles
    {
        public const String Tabla_Ope_Alm_Ajustes_Almacen = "OPE_ALM_AJUSTES_DETALLES";
        public const String Campo_No_Ajuste = "NO_AJUSTE";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Existencia_Sistema = "EXISTENCIA_SISTEMA";
        public const String Campo_Conteo_Fisico = "CONTEO_FISICO";
        public const String Campo_Diferencia = "DIFERENCIA";
        public const String Campo_Tipo_Movimiento = "TIPO_MOVIMIENTO";
        public const String Campo_Importe_Diferencia = "IMPORTE_DIFERENCIA";
        public const String Campo_Precio_Promedio = "PRECIO_PROMEDIO";
        public const String Campo_Nombre_Descipcion = "NOMBRE_DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Alm_Com_Salidas
    /// DESCRIPCIÓN:        Clase que contiene los campos de la tabla ALM_COM_SALIDAS
    /// PARÁMETROS :     
    /// CREO       :        Noe Mosqueda Valadez
    /// FECHA_CREO :        11/Noviembre/2010 16:44 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Alm_Com_Salidas
    {
        public const String Tabla_Alm_Com_Salidas = "ALM_COM_SALIDAS";
        public const String Campo_No_Salida = "NO_SALIDA";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Area_ID = "AREA_ID";
        public const String Campo_Empleado_Solicito_ID = "EMPLEADO_SOLICITO_ID";
        public const String Campo_Requisicion_ID = "NO_REQUISICION";
        public const String Campo_Tipo_Salida_ID = "TIPO_SALIDA_ID";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
        public const String Campo_Subtotal = "SUBTOTAL";
        public const String Campo_IVA = "IVA";
        public const String Campo_Total = "TOTAL";

        public const String Campo_Poliza_Stock_SAP = "POLIZA_SAP";
        public const String Campo_Contabilizado = "CONTABILIZADO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Alm_Com_Salidas_Detalles
    /// DESCRIPCIÓN:        Clase que contiene los campos de la tabla ALM_COM_SALIDAS_DETALLES
    /// PARÁMETROS :     
    /// CREO       :        Noe Mosqueda Valadez
    /// FECHA_CREO :        11/Noviembre/2010 16:50 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Alm_Com_Salidas_Detalles
    {
        public const String Tabla_Alm_Com_Salidas_Detalles = "ALM_COM_SALIDAS_DETALLES";
        public const String Campo_No_Salida = "NO_SALIDA";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Costo = "COSTO";
        public const String Campo_Costo_Promedio = "COSTO_PROMEDIO";
        public const String Campo_Subtotal = "SUBTOTAL";
        public const String Campo_IVA = "IVA";
        public const String Campo_Importe = "IMPORTE";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Com_Recibos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_RECIBOS
    /// PARAMETROS :     
    /// CREO       :           Noe Mosqueda Valadez
    /// FECHA_CREO :           16/Noviembre/2010 10:35
    /// MODIFICO          :    
    /// FECHA_MODIFICO    :    
    /// CAUSA_MODIFICACION:    
    ///*******************************************************************************/
    public class Ope_Com_Recibos
    {
        public const String Tabla_Ope_Com_Recibos = "OPE_COM_RECIBOS";
        public const String Campo_No_Recibo = "NO_RECIBO";
        public const String Campo_No_Salida = "NO_SALIDA";
        public const String Campo_Empleado_Recibo_ID = "EMPLEADO_RECIBO_ID";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Com_Resguardos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_RESGUARDOS
    /// PARAMETROS :     
    /// CREO       :           Noe Mosqueda Valadez
    /// FECHA_CREO :           16/Noviembre/2010 10:41
    /// MODIFICO          :    
    /// FECHA_MODIFICO    :    
    /// CAUSA_MODIFICACION:    
    ///*******************************************************************************/
    public class Ope_Com_Resguardos
    {
        public const String Tabla_Ope_Com_Resguardos = "OPE_COM_RESGUARDOS";
        public const String Campo_No_Resguardo = "NO_RESGUARDO";
        public const String Campo_No_Salida = "NO_SALIDA";
        public const String Campo_Empleado_Resguardo_ID = "EMPLEADO_RESGUARDO_ID";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Listado
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla Ope_Com_Listado
    /// PARÁMETROS :     
    /// CREO       : Susana Trigueros Armenta 
    /// FECHA_CREO : 20/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Listado
    {
        public const String Tabla_Ope_Com_Listado = "OPE_COM_LISTADO";
        public const String Campo_Listado_ID = "LISTADO_ID";
        public const String Campo_No_Requisicion_ID = "NO_REQUISICION";
        public const String Campo_No_Partida_ID = "NO_PARTIDA_ID";
        public const String Campo_No_Proyecto_ID = "NO_PROYECTO_ID";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Total = "TOTAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Fecha_Construccion = "FECHA_CONSTRUCCION";
        public const String Campo_Empleado_Construccion_ID = "EMPLEADO_CONSTRUCCION_ID";
        public const String Campo_Fecha_Generacion = "FECHA_GENERACION";
        public const String Campo_Empleado_Generacion_ID = "EMPLEADO_GENERACION_ID";
        public const String Campo_Fecha_Autorizacion = "FECHA_AUTORIZACION";
        public const String Campo_Empleado_Autorizacion_ID = "EMPLEADO_AUTORIZACION_ID";
        public const String Campo_Fecha_Cancelacion = "FECHA_CANCELACION";
        public const String Campo_Empleado_Cancelacion_ID = "EMPLEADO_CANCELACION_ID";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Req_Observaciones
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_COM_REQ_OBSERVACIONES
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            08/Diciembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Alm_Com_Obs_Listado
    {
        public const String Tabla_Ope_Alm_Com_Obs_Listados = "OPE_ALM_COM_OBS_LISTADO";
        public const String Campo_Obs_listado_ID = "OBS_LISTADO_ID";
        public const String Campo_No_Listado_ID = "NO_LISTADO_ID";
        public const String Campo_Comentario = "COMENTARIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Listado_Detalle
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla Ope_Com_Listado_Detalle
    /// PARÁMETROS :     
    /// CREO       : Susana Trigueros Armenta 
    /// FECHA_CREO : 20/Noviembre/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Listado_Detalle
    {
        public const String Tabla_Ope_Com_Listado_Detalle = "OPE_COM_LISTADO_DETALLE";
        public const String Campo_No_Listado_ID = "NO_LISTADO_ID";
        public const String Campo_No_Producto_ID = "NO_PRODUCTO_ID";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Costo_Compra = "COSTO_COMPRA";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Monto_IVA = "MONTO_IVA";
        public const String Campo_Monto_IEPS = "MONTO_IEPS";
        public const String Campo_Porcentaje_IVA = "PORCENTAJE_IVA";
        public const String Campo_Porcentaje_IEPS = "PORCENTAJE_IEPS";
        public const String Campo_No_Requisicion = "NO_REQUISICION";
        public const String Campo_Borrado = "BORRADO";
        public const String Campo_Motivo_Borrado = "MOTIVO_BORRADO";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Alm_Com_Entradas
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla Alm_Com_Entradas
    /// PARÁMETROS :     
    /// CREO       :            Noe Mosqueda Valadez 
    /// FECHA_CREO :            24/Noviembre/2010 17:56 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Alm_Com_Entradas
    {
        public const String Tabla_Alm_Com_Entradas = "ALM_COM_ENTRADAS";
        public const String Campo_No_Entrada = "NO_ENTRADA";
        public const String Campo_Tipo_Entrada_ID = "TIPO_ENTRADA_ID";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Total = "TOTAL";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Factura_Interno = "NO_FACTURA_INTERNO";
        public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Alm_Com_Entradas_Detalles
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla Alm_Com_Entradas_Detalles
    /// PARÁMETROS :     
    /// CREO       :            Noe Mosqueda Valadez 
    /// FECHA_CREO :            24/Noviembre/2010 18:00 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Alm_Com_Entradas_Detalles
    {
        public const String Tabla_Alm_Com_Entradas_Detalles = "ALM_COM_ENTRADAS_DETALLES";
        public const String Campo_No_Entrada = "NO_ENTRADA";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Costo_Compra = "COSTO_COMPRA";
        public const String Campo_Costo_Promedio = "COSTO_PROMEDIO";
        public const String Campo_No_Resguardo = "NO_RESGUARDO";
        public const String Campo_No_Recibo = "NO_RECIBO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Com_Facturas_Proveedores
    /// DESCRIPCIÓN:            Clase que contiene los campos de la tabla Ope_Com_Facturas_Proveedores
    /// PARÁMETROS :     
    /// CREO       :            Noe Mosqueda Valadez 
    /// FECHA_CREO :            26/Noviembre/2010 12:02 
    /// MODIFICO          :     Salvador Hernández Ramírez
    /// FECHA_MODIFICO    :     01/Marzo/2011
    /// CAUSA_MODIFICACIÓN:     Se agregaron los campos "Empleado_Almacen_ID" y "No_ContraRecibo" 
    ///*******************************************************************************
    public class Ope_Com_Facturas_Proveedores
    {
        public const String Tabla_Ope_Com_Facturas_Proveedores = "OPE_COM_FACTURAS_PROVEEDORES";
        public const String Campo_No_Factura_Interno = "NO_CONTRA_RECIBO";
        public const String Campo_No_Factura_Proveedor = "NO_FACTURA_PROVEEDOR";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Fecha_Factura = "FECHA_FACTURA";
        public const String Campo_Fecha_Recepcion = "FECHA_RECEPCION";
        public const String Campo_Fecha_Pago = "FECHA_PAGO";
        public const String Campo_SubTotal_Con_Impuesto = "SUBTOTAL_CON_IMPUESTO";
        public const String Campo_SubTotal_Sin_Impuesto = "SUBTOTAL_SIN_IMPUESTO";
        public const String Campo_IVA = "IVA";
        public const String Campo_IEPS = "IEPS";
        public const String Campo_Total = "TOTAL";
        public const String Campo_Abono = "ABONO";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Fecha_Cancelacion = "FECHA_CANCELACION";
        public const String Campo_Motivo_Cancelacion = "MOTIVO_CANCELACION";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
        public const String Campo_No_ContraRecibo = "NO_CONTRARECIBO";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Alm_Com_Contrarecibos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla ALM_COM_CONTRARECIBOS
    /// PARAMETROS :     
    /// CREO       :           Noe Mosqueda Valadez
    /// FECHA_CREO :           29/Diciembre/2010 11:51 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Alm_Com_Contrarecibos
    {
        public const String Tabla_Alm_Com_Contrarecibos = "ALM_COM_CONTRARECIBOS";
        public const String Campo_No_ContraRecibo = "NO_CONTRARECIBO";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
        public const String Campo_Fecha_Recepcion = "FECHA_RECEPCION";
        public const String Campo_Fecha_Pago = "FECHA_PAGO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Observaciones = "OBSERVACIONES";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Alm_Com_Contrarecibos_Detalles
    /// DESCRIPCION:           Clase que contiene los campos de la tabla ALM_COM_CONTRARECIBOS_DETALLES
    /// PARAMETROS :     
    /// CREO       :           Noe Mosqueda Valadez
    /// FECHA_CREO :           29/Diciembre/2010 11:51 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Alm_Com_Contrarecibos_Detalles
    {
        public const String Tabla_Alm_Com_Contrarecibos_Detalles = "ALM_COM_CONTRARECIBOS_DETALLES";
        public const String Campo_No_ContraRecibo = "NO_CONTRARECIBO";
        public const String Campo_No_Factura_Proveedor = "NO_FACTURA_PROVEEDOR";
        public const String Campo_No_Partida = "NO_PARTIDA";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Com_Det_Doc_Soporte
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_DET_DOC_SOPORTE
    /// PARAMETROS :     
    /// CREO       :           Salvador Hernández Ramírez
    /// FECHA_CREO :           17/Marzo/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Com_Det_Doc_Soporte
    {
        public const String Tabla_Ope_Com_Det_Doc_Soporte = "OPE_COM_DET_DOC_SOPORTE";
        public const String Campo_Documento_ID = "DOCUMENTO_ID";
        public const String Campo_No_Factura_Interno = "NO_CONTRA_RECIBO";
        public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Marbete = "MARBETE";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:  Ope_Com_Series_Productos
    /// DESCRIPCIÓN:         Clase que contiene los campos de la tabla OPE_COM_SERIES_PRODUCTOS
    /// PARÁMETROS :     
    /// CREO       :         Salvador Hernández Ramírez
    /// FECHA_CREO :         24/Marzo/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Series_Productos
    {
        public const String Tabla_Ope_Com_Series_Productos = "OPE_COM_SERIES_PRODUCTOS";
        public const String Campo_No_Serie = "NO_SERIE";
        public const String Campo_No_Orden_Compra = "NO_ORDEN_COMPRA";
        public const String Campo_Producto_Id = "PRODUCTO_ID";
        public const String Campo_Marca_Id = "MARCA_ID";
        public const String Campo_Modelo_Id = "MODELO_ID";
        public const String Campo_Serie = "SERIE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Recibo = "NO_RECIBO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Alm_Productos_Contrarecibo   
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_ALM_PROD_CONTRARECIBO
    /// PARAMETROS :     
    /// CREO       :           Salvador Hernández Ramírez
    /// FECHA_CREO :           03/Julio/2011 
    /// MODIFICO          :    
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    ///

    public class Ope_Alm_Productos_Contrarecibo
    {
        public const String Tabla_Ope_Alm_Productos_Contrarecibo = "OPE_ALM_PROD_CONTRARECIBO";
        public const String Campo_No_Registro = "NO_REGISTRO";
        public const String Campo_No_Contra_Recibo = "NO_CONTRA_RECIBO";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Resguardo = "RESGUARDO";
        public const String Campo_Recibo = "RECIBO";
        public const String Campo_Unidad = "UNIDAD";
        public const String Campo_Totalidad = "TOTALIDAD";
        public const String Campo_Recibo_Transitorio = "RECIBO_TRANSITORIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Registrado = "REGISTRADO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Alm_Registro_Facturas   
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_ALM_REGISTRO_FACTURAS
    /// PARAMETROS :     
    /// CREO       :           Salvador Hernández Ramírez
    /// FECHA_CREO :           03/Julio/2011 
    /// MODIFICO          :    
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Alm_Registro_Facturas
    {
        public const String Tabla_Ope_Alm_Registro_Facturas = "OPE_ALM_REGISTRO_FACTURAS";
        public const String Campo_Factura_ID = "FACTURA_ID";
        public const String Campo_Factura_Proveedor = "NO_FACTURA_PROVEEDOR";
        public const String Campo_No_Contra_Recibo = "NO_CONTRA_RECIBO";
        public const String Campo_Importe_Factura = "IMPORTE_FACTURA";
        public const String Campo_Fecha_Factura = "FECHA_FACTURA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
    }



    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:  Ope_Alm_Pat_Inv_Bienes_Muebles
    /// DESCRIPCIÓN:         Clase que contiene los campos de la tabla OPE_ALM_PAT_INV_B_MUEBLES
    /// PARÁMETROS :     
    /// CREO       :         Salvador Hernández Ramírez
    /// FECHA_CREO :         04/Julio/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Alm_Pat_Inv_Bienes_Muebles
    {
        public const String Tabla_Ope_Alm_Pat_Inv_Bienes_Muebles = "OPE_ALM_PAT_INV_B_MUEBLES";
        public const String Campo_No_Inventario = "NO_INVENTARIO";
        public const String Campo_Inventario = "INVENTARIO";
        public const String Campo_Producto_Id = "PRODUCTO_ID";
        public const String Campo_Marca_Id = "MARCA_ID";
        public const String Campo_Modelo_Id = "MODELO_ID";
        public const String Campo_Color_Id = "COLOR_ID";
        public const String Campo_Material_Id = "MATERIAL_ID";
        public const String Campo_No_Serie = "NO_SERIE";
        public const String Campo_No_Contra_Recibo = "NO_CONTRA_RECIBO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Observaciones = "OBSERVACIONES";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Resguardado = "RESGUARDADO";

        public const String Campo_Modelo = "MODELO";
        public const String Campo_Garantia = "GARANTIA";
        public const String Campo_Operacion = "OPERACION";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:  Ope_Alm_Pat_Inv_Vehiculos
    /// DESCRIPCIÓN:         Clase que contiene los campos de la tabla OPE_ALM_PAT_INV_VEHICULOS
    /// PARÁMETROS :     
    /// CREO       :         Salvador Hernández Ramírez
    /// FECHA_CREO :         04/Julio/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Alm_Pat_Inv_Vehiculos
    {
        public const String Tabla_Ope_Alm_Pat_Inv_Vehiculos = "OPE_ALM_PAT_INV_VEHICULOS";
        public const String Campo_No_Inventario = "NO_INVENTARIO";
        public const String Campo_Inventario = "INVENTARIO";
        public const String Campo_Producto_Id = "PRODUCTO_ID";
        public const String Campo_Marca_Id = "MARCA_ID";
        public const String Campo_Modelo_Id = "MODELO_ID";
        public const String Campo_Color_Id = "COLOR_ID";
        public const String Campo_Material_Id = "MATERIAL_ID";
        public const String Campo_No_Serie = "NO_SERIE";
        public const String Campo_No_Contra_Recibo = "NO_CONTRA_RECIBO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Modelo = "MODELO";
        public const String Campo_Descripcion = "DESCRIPCION";
    }


    #endregion
    #endregion

    ///*************************************************************************************************************************
    ///                                                                ALMACÉN
    ///*************************************************************************************************************************

    #region Región Almacén

    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Cat_Alm_Parametros_Listado
    ///DESCRIPCIÓN:        Clase que contiene los campos de la tabla CAT_ALM_PARAMETROS_LISTADO
    ///PARAMETROS: 
    ///CREO:               Susana Trigueros Armenta
    ///FECHA_CREO:         15-Oct-12
    ///MODIFICO: 
    ///FECHA_MODIFICO:
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Alm_Parametros_Listado
    {
        public const String Tabla_Cat_Alm_Parametros_Listado = "CAT_ALM_PARAMETROS_LISTADO";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Unidad_Responsable_ID = "UNIDAD_RESPONSABLE_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Subnivel_ID = "SUBNIVEL_ID";
    }

    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Ope_Alm_Recibos_Transitorios
    ///DESCRIPCIÓN:        Clase que contiene los campos de la tabla OPE_ALM_RECIBOS_TRANSITORIOS_T
    ///PARAMETROS: 
    ///CREO:               Salvador Hernández Ramírez
    ///FECHA_CREO:         26-Julio-11
    ///MODIFICO: 
    ///FECHA_MODIFICO:
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Alm_Recibos_Transitorios_Totalidad
    {
        public const String Tabla_Ope_Alm_Recibos_Transitorios_Totalidad = "OPE_ALM_RECIBOS_TRANSITORIOS_T";
        public const String Campo_No_Recibo = "NO_RECIBO";
        public const String Campo_No_Contra_Recibo = "NO_CONTRA_RECIBO";
        public const String Campo_Responsable_ID = "RESPONSABLE_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Tipo = "TIPO";
    }

    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Ope_Alm_Recibos_Transitorios
    ///DESCRIPCIÓN:        Clase que contiene los campos de la tabla OPE_ALM_RECIBOS_TRANSITORIOS
    ///PARAMETROS: 
    ///CREO:               Salvador Hernández Ramírez
    ///FECHA_CREO:         09-Julio-11
    ///MODIFICO: 
    ///FECHA_MODIFICO:
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Alm_Recibos_Transitorios
    {
        public const String Tabla_Ope_Alm_Recibos_Transitorios = "OPE_ALM_RECIBOS_TRANSITORIOS";
        public const String Campo_No_Recibo = "NO_RECIBO";
        public const String Campo_No_Contra_Recibo = "NO_CONTRA_RECIBO";
        public const String Campo_Responsable_ID = "RESPONSABLE_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Tipo = "TIPO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Cap_Inv_Stock
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_COM_CAP_INV_STOCK
    /// PARÁMETROS :     
    /// CREO       : Salvador Hernández Ramírez
    /// FECHA_CREO : 13/Enero/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************

    public class Ope_Com_Cap_Inv_Stock
    {
        public const String Tabla_Ope_Com_Cap_Inv_Stock = "OPE_COM_CAP_INV_STOCK";
        public const String Campo_No_Inventario = "NO_INVENTARIO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Observaciones = "OBSERVACIONES";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Com_Cap_Stock_Detalles
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_COM_CAP_STOCK_DETALLES
    /// PARÁMETROS :     
    /// CREO       : Salvador Hernández Ramírez
    /// FECHA_CREO : 13/Enero/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Com_Cap_Stock_Detalles
    {
        public const String Tabla_Ope_Com_Cap_Stock_Detalles = "OPE_COM_CAP_STOCK_DETALLES";
        public const String Campo_No_Inventario = "NO_INVENTARIO";
        public const String Campo_Producto_Id = "PRODUCTO_ID";
        public const String Campo_Contados_Sistema = "CONTADOS_SISTEMA";
        public const String Campo_Contados_Usuario = "CONTADOS_USUARIO";
        public const String Campo_Diferencia = "DIFERENCIA";
        public const String Campo_Marbete = "MARBETE";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Com_Ajustes_Inv_Stock
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_AJUSTES_INV_STOCK
    /// PARAMETROS :     
    /// CREO       :           Salvador Hernández Ramírez
    /// FECHA_CREO :           25/Enero/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Com_Ajustes_Inv_Stock
    {
        public const String Tabla_Ope_Com_Ajustes_Inv_Stock = "OPE_COM_AJUSTES_INV_STOCK";
        public const String Campo_No_Inventario = "NO_INVENTARIO";
        public const String Campo_Tipo_Ajuste = "TIPO_AJUSTE";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario_Ajusto = "USUARIO_AJUSTO";
        public const String Campo_No_Empleado = "NO_EMPLEADO";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Marbete = "MARBETE";
    }

    #endregion Almacén

    ///*************************************************************************************************************************
    ///                                                                ARMONIZACIN
    ///*************************************************************************************************************************

    #region ARMONIZACION
    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Cat_SAP_Fuente_Financiamiento
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla CAT_SAP_FTE_FINANCIAMIENTO
    /// 	PARÁMETROS:
    /// 	CREO: Roberto González Oseguera
    /// 	FECHA_CREO: 25-feb-2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    public class Cat_SAP_Fuente_Financiamiento
    {
        public const String Tabla_Cat_SAP_Fuente_Financiamiento = "CAT_SAP_FTE_FINANCIAMIENTO";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Especiales_Ramo_33 = "ESPECIALES_RAMO_33";
    }
    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Cat_SAP_Capitulos
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla CAT_SAP_Capitulo
    /// 	PARÁMETROS:
    /// 	CREO: Jacqueline Ramírez Sierra
    /// 	FECHA_CREO: 25-feb-2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    public class Cat_SAP_Capitulos
    {
        public const String Tabla_Cat_SAP_Capitulos = "CAT_SAP_CAPITULO";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Cat_SAP_Concepto
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla CAT_SAP_Capitulo
    /// 	PARÁMETROS:
    /// 	CREO:
    /// 	FECHA_CREO: 25-feb-2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    public class Cat_Sap_Concepto
    {
        public const String Tabla_Cat_SAP_Concepto = "CAT_SAP_CONCEPTO";
        public const String Campo_Concepto_ID = "CONCEPTO_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Cat_SAP_Partida_Generica
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla CAT_SAP_PARTIDA_GENERICA
    /// 	PARÁMETROS:
    /// 	CREO:	Roberto González Oseguera
    /// 	FECHA_CREO: 28-feb-2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    public class Cat_SAP_Partida_Generica
    {
        public const String Tabla_Cat_SAP_Partida_Generica = "CAT_SAP_PARTIDA_GENERICA";
        public const String Campo_Partida_Generica_ID = "PARTIDA_GENERICA_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Concepto_ID = "CONCEPTO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Cat_Sap_Partidas_Especificas
    ///DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_SAP_PARTIDAS_ESPECIFICAS
    ///PARAMETROS: 
    ///CREO: jesus toledo
    ///FECHA_CREO: 03/03/2011 12:36:44 p.m.
    ///MODIFICO: 
    ///FECHA_MODIFICO:
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************

    public class Cat_Sap_Partidas_Especificas
    {
        public const String Tabla_Cat_SAP_Partidas_Especificas = "CAT_SAP_PARTIDAS_ESPECIFICAS";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Giro_ID = "GIRO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Partida_Generica_ID = "PARTIDA_GENERICA_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Cuenta = "CUENTA";
    }
    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Cat_SAP_Area_Funcional
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla CAT_SAP_AREA_FUNCIONAL
    /// 	PARÁMETROS:
    /// 	CREO: Leslie González Vázquez
    /// 	FECHA_CREO: 25/Feb/2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    ///
    public class Cat_SAP_Area_Funcional
    {
        public const String Tabla_Cat_SAP_Area_Funcional = "CAT_SAP_AREA_FUNCIONAL";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Anio = "ANIO";
    }

    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Cat_OPE_SAP_PRES_PROG_PROY
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla OPE_SAP_PRES_PROG_PROY
    /// 	PARÁMETROS:
    /// 	CREO: Leslie González Vázquez
    /// 	FECHA_CREO: 01/Marzo/2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    public class Ope_SAP_Pres_Prog_Proy
    {
        public const String Tabla_Ope_SAP_Pres_Prog_Proy = "OPE_SAP_PRES_PROG_PROY";
        public const String Campo_Pres_Prog_Proy_ID = "PRES_PROG_PROY_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Monto_Presupuestal = "MONTO_PRESUPUESTAL";
        public const String Campo_Monto_Disponible = "MONTO_DISPONIBLE";
        public const String Campo_Monto_Comprometido = "MONTO_COMPROMETIDO";
        public const String Campo_Anio_Presupuesto = "ANIO_PRESUPUESTO";
        public const String Campo_Monto_Ejercido = "MONTO_EJERCIDO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Sap_Det_Prog_Dependencia
    /// DESCRIPCION:           
    /// PARAMETROS :     
    /// CREO       : Susana Trigueros Armenta
    /// FECHA_CREO :       28 Feb 2011    
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Sap_Det_Prog_Dependencias
    {
        public const String Tabla_Cat_Sap_Det_Prog_Dependencias = "CAT_SAP_DET_PROG_DEPENDENCIA";
        public const String Campo_Prog_Dependencia_ID = "PROG_DEPENDENCIA_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Sap_Partidas_Genericas
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_SAP_PARTIDA_GENERICA
    /// PARÁMETROS :
    /// CREO       : Juan Alberto Hernández Negrete
    /// FECHA_CREO : 25/Feebrero/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Sap_Partidas_Genericas
    {
        public const String Tabla_Cat_Sap_Partidas_Genericas = "CAT_SAP_PARTIDA_GENERICA";
        public const String Campo_Partida_Generica_ID = "PARTIDA_GENERICA_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Concepto_ID = "CONCEPTO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Cat_SAP_Det_Fte_Dependencia
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla CAT_SAP_DET_FTE_DEPENDENCIA
    /// 	PARÁMETROS:
    /// 	CREO:	Roberto González Oseguera
    /// 	FECHA_CREO: 04-mar-2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    public class Cat_SAP_Det_Fte_Dependencia
    {
        public const String Tabla_Cat_SAP_Det_Fte_Financiamiento_Dependencia = "CAT_SAP_DET_FTE_DEPENDENCIA";
        public const String Campo_Financiamiento_Dependencia_ID = "FINANCIAMIENTO_DEPENDENCIA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
    }

    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Cat_SAP_Det_Prog_Dependencia
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla CAT_SAP_DET_PROG_DEPENDENCIA
    /// 	PARÁMETROS:
    /// 	CREO:	Roberto González Oseguera
    /// 	FECHA_CREO: 04-mar-2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    public class Cat_SAP_Det_Prog_Dependencia
    {
        public const String Tabla_Cat_SAP_Det_Programa_Dependencia = "CAT_SAP_DET_PROG_DEPENDENCIA";
        public const String Campo_Programa_Dependencia_ID = "PROG_DEPENDENCIA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
    }

    ///*******************************************************************************************************
    /// 	NOMBRE_CLASE: Ope_SAP_Parametros
    /// 	DESCRIPCIÓN: La clase contiene los campos de la tabla OPE_SAP_PARAMETROS
    /// 	PARÁMETROS:
    /// 	CREO:	Roberto González Oseguera
    /// 	FECHA_CREO: 18-abr-2011
    /// 	MODIFICÓ: 
    /// 	FECHA_MODIFICÓ: 
    /// 	CAUSA_MODIFICACIÓN: 
    ///*******************************************************************************************************
    public class Ope_SAP_Parametros
    {
        public const String Tabla_Ope_SAP_Parametros = "OPE_SAP_PARAMETROS";
        public const String Campo_Sociedad = "SOCIEDAD";
        public const String Campo_Fondo = "FONDO";
        public const String Campo_Division = "DIVISION";
    }
    #endregion
    
    #region GASTOS

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Com_Gastos
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_GASTOS 
    /// PARAMETROS :     
    /// CREO       :           Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO :           26/Marzo/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Com_Gastos
    {
        public const String Tabla_Ope_Com_Gastos = "OPE_COM_GASTOS";
        public const String Campo_Gasto_ID = "GASTO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Fecha_Solicitud = "FECHA_SOLICITUD";
        public const String Campo_Costo_Total_Gasto = "COSTO_TOTAL_GASTO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Numero_Reserva = "NUMERO_RESERVA";
        public const String Campo_Fecha_Asig_Numero_Reserva = "FECHA_ASIG_NUMERO_RESERVA";
        public const String Campo_Usuario_Asig_Numero_Reserva = "USUARIO_ASIG_NUM_RESERVA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Folio = "FOLIO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Ieps = "IEPS";
        public const String Campo_Iva = "IVA";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_No_Factura = "NO_FACTURA";
        public const String Campo_Fecha_Factura = "FECHA_FACTURA";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Com_Gastos_Detalles
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_GASTOS_DETALLES 
    /// PARAMETROS :     
    /// CREO       :           Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO :           26/Marzo/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Com_Gastos_Detalles
    {
        public const String Tabla_Ope_Com_Gastos_Detalles = "OPE_COM_GASTOS_DETALLES";
        public const String Campo_Gasto_Detalle_ID = "GASTO_DETALLE_ID";
        public const String Campo_Gasto_ID = "GASTO_ID";
        //public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Costo = "COSTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        //public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Producto_Servicio = "PRODUCTO_SERVICIO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Ieps = "IEPS";
        public const String Campo_Iva = "IVA";
        public const String Campo_Monto_Total = "MONTO_TOTAL";
        public const String Campo_Identificador = "IDENTIFICADOR";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Com_Gastos_Comentarios
    /// DESCRIPCION:           Clase que contiene los campos de la tabla 
    ///                        OPE_COM_GASTOS_DETALLES 
    /// PARAMETROS :     
    /// CREO       :           Francisco Antonio Gallardo Castañeda.
    /// FECHA_CREO :           26/Marzo/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Com_Gastos_Comentarios
    {
        public const String Tabla_Ope_Com_Gastos_Comentarios = "OPE_COM_GASTOS_COMENTARIOS";
        public const String Campo_Gasto_Comentario_ID = "GASTO_COMENTARIO_ID";
        public const String Campo_Gasto_ID = "GASTO_ID";
        public const String Campo_Fecha_Comentario = "FECHA_COMENTARIO";
        public const String Campo_Usuario_Comentario = "USUARIO_COMENTO";
        public const String Campo_Comentario = "COMENTARIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    #endregion


    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Ope_Pre_Empleados_Activos
    ///DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PRE_EMPLEADOS_ACTIVOS
    ///PARAMETROS: 
    ///CREO: jesus toledo
    ///FECHA_CREO: 03/31/2011 01:37:25 p.m.
    ///MODIFICO: 
    ///FECHA_MODIFICO:
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Pre_Empleados_Activos
    {
        public const String Tabla_Ope_Pre_Empleados_Activos = "OPE_PRE_EMPLEADOS_ACTIVOS";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Consecutivo = "CONSECUTIVO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:   Ope_Com_Sincronizaciones_Presupuesto
    /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COM_SINCRONIZACIONES_PRE
    /// PARAMETROS :     
    /// CREO       :           Roberto González OSeguera
    /// FECHA_CREO :           13-abr-2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Ope_Com_Sincronizaciones_Presupuesto
    {
        public const String Tabla_Ope_Com_Sincronizaciones_Presupuesto = "OPE_COM_SINCRONIZACIONES_PRE";
        public const String Campo_No_Sincronizacion = "NO_SINCRONIZACION";
        public const String Campo_Nombre_Archivo = "NOMBRE_ARCHIVO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:   Cat_Com_Tipos_Resguardo
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COM_TIPOS_RESGUARDO
    /// PARAMETROS :     
    /// CREO       :           Jennyfer Ivonne Ceja Lemus
    /// FECHA_CREO :           05-junio-2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Com_Tipos_Resguardo
    {
        public const String Tabla_Cat_Com_Tipos_Resguardo = "CAT_COM_TIPOS_RESGUARDO";
        public const String Campo_Tipo_Resguardo_ID = "TIPO_RESGUARDO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
    }

    ///*************************************************************************************************************************
    ///                                                                CONTABILIDAD
    ///*************************************************************************************************************************
    ///

    #region Contabilidad

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Solicitud_Finiquito_Det
    /// DESCRIPCION         : Clase con contiene los datos de la tabla OPE_CON_SOLICITUD_FINIQUITO_DET
    /// PARAMETROS          :
    /// CREO                : Leslie González Vázquez
    /// FECHA_CREO          : 08/Agosto/2013
    /// MODIFICO            :
    /// FECHA_MODIFICO      :
    /// CAUSA_MODIFICACION  :
    ///*******************************************************************************
    public class Ope_Con_Solicitud_Finiquito_Det
    {
        public const String Tabla_Ope_Con_Solicitud_Finiquito_Det = "OPE_CON_SOLICITUD_FINIQUITO_DET";
        public const String Campo_No_Detalle = "NO_DETALLE";
        public const String Campo_No_Solicitud_Pago = "NO_SOLICITUD_PAGO";
        public const String Campo_Percepcion_Deduccion_ID = "PERCEPCION_DEDUCCION_ID";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Debe = "DEBE";
        public const String Campo_Haber = "HABER";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /////*******************************************************************************
    ///// NOMBRE DE LA CLASE: Cat_Con_Parametros_Cuentas_Orden_Egresos
    ///// DESCRIPCIÓN: Clase que contiene los campos de la tabla Cat_Con_Parametros_Cuentas_Orden_Egresos
    ///// PARÁMETROS :     
    ///// CREO       : Susana Trigueros Armenta 
    ///// FECHA_CREO : 03/Julio/2010 
    ///// MODIFICO          :
    ///// FECHA_MODIFICO    :
    ///// CAUSA_MODIFICACIÓN:
    /////*******************************************************************************
    public class Cat_Con_Parametros_Cuentas_Orden_Egresos
    {
        public const String Tabla_Cat_Con_Parametros_Cuentas_Orden_Egresos = "CAT_CON_PAR_CUENTAS_ORDEN_EGRESOS";
        public const String Campo_ID = "ID";
        public const String Campo_Momento_Presupuestal = "MOMENTO_PRESUPUESTAL";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    /////*******************************************************************************
    ///// NOMBRE DE LA CLASE: Cat_Con_Parametros_Cuentas_Orden_Ingresos
    ///// DESCRIPCIÓN: Clase que contiene los campos de la tabla Cat_Con_Parametros_Cuentas_Orden_Ingresos
    ///// PARÁMETROS :     
    ///// CREO       : Susana Trigueros Armenta 
    ///// FECHA_CREO : 03/Julio/2010 
    ///// MODIFICO          :
    ///// FECHA_MODIFICO    :
    ///// CAUSA_MODIFICACIÓN:
    /////*******************************************************************************
    public class Cat_Con_Parametros_Cuentas_Orden_Ingresos
    {
        public const String Tabla_Cat_Con_Parametros_Cuentas_Orden_Ingresos = "CAT_CON_PAR_CUENTAS_ORDEN_INGRESOS";
        public const String Campo_ID = "ID";
        public const String Campo_Momento_Presupuestal = "MOMENTO_PRESUPUESTAL";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Con_Cheques
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla Ope_Con_Cheques
    ///PARAMETROS           :
    ///CREO                 : Jennyfer Ivonne Ceja Lemus
    ///FECHA_CREO           : 15/DICIEMBRE/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Con_Cheques
    {
        public const String Tabla_Ope_Con_Cheques = "Ope_Con_Cheques";
        public const String Campo_No_Cheque = "No_Cheque";
        public const String Campo_Folio = "Folio";
        public const String Campo_Banco_ID = "Banco_ID";
        public const String Campo_No_Pago = "No_Pago";
        public const String Campo_Fecha = "Fecha";
        public const String Campo_Estatus = "Estatus";
        public const String Campo_Concepto = "Concepto";
        public const String Campo_Monto = "Monto";
        public const String Campo_Monto_Letra = "Monto_Letra";
        public const String Campo_Beneficiario = "Beneficiario";
        public const String Campo_Usuario_Creo = "Usuario_Creo";
        public const String Campo_Fecha_Creo = "Fecha_Creo";
        public const String Campo_Usuario_Modifico = "Usuario_Modifico";
        public const String Campo_Fecha_Modifico = "Fecha_Modifico";
    }
    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : [Ope_Con_Seguimiento_Cheques]
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla [Ope_Con_Seguimiento_Cheques]
    ///PARAMETROS           :
    ///CREO                 : Jennyfer Ivonne Ceja Lemus
    ///FECHA_CREO           : 15/DICIEMBRE/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Con_Seguimiento_Cheques
    {
        public const String Tabla_Ope_Con_Seguimiento_Cheques = "Ope_Con_Seguimiento_Cheques";
        public const String Campo_No_Cheque = "No_Cheque";
        public const String Campo_Empleado_Recibio_ID = "Empleado_Recibio_ID";
        public const String Campo_Fecha_Entrada = "Fecha_Entrada";
        public const String Campo_Fecha_Salida = "Fecha_Salida";
        public const String Campo_Estatus_Entrada = "Estatus_Entrada";
        public const String Campo_Estatus_Salida = "Estatus_Salida";
        public const String Campo_Comentarios = "Comentarios";
        public const String Campo_Usuario_Creo = "Usuario_Creo";
        public const String Campo_Fecha_Creo = "Fecha_Creo";
        public const String Campo_Usuario_Modifico = "Usuario_Modifico";
        public const String Campo_Fecha_Modifico = "Fecha_Modifico";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Cierre_Anual
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_CON_CIERRE_ANUAL
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 06/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Cierre_Anual
    {
        public const String Tabla_Ope_Con_Cierre_Anual = "OPE_CON_CIERRE_ANUAL";
        public const String Campo_No_Cierre_Anual = "NO_CIERRE_ANUAL";
        public const String Campo_Cuenta_Contable_ID_Inicio = "CUENTA_CONTABLE_ID_INICIO";
        public const String Campo_Cuenta_Contable_ID_Fin = "CUENTA_CONTABLE_ID_FIN";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Total_Debe = "TOTAL_DEBE";
        public const String Campo_Total_Haber = "TOTAL_HABER";
        public const String Campo_Diferencia = "DIFERENCIA";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Con_Tipo_Polizas
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_CON_TIPO_POLIZAS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 06/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Tipo_Polizas
    {
        public const String Tabla_Cat_Con_Tipo_Polizas = "CAT_CON_TIPO_POLIZAS";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Abreviatura = "ABREVIATURA";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: CAT_CON_MATRIZ_DE_CUENTAS
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_CON_MATRIZ_DE_CUENTAS
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 27/Diciembre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Matriz_De_Cuentas
    {
        public const String Tabla_Cat_Con_Matriz_Cuentas = "CAT_CON_MATRIZ_DE_CUENTAS";
        public const String Campo_No_Matriz = "NO_MATRIZ";
        public const String Campo_Matriz_ID = "MATRIZ_ID";
        public const String Campo_Cuenta_Abono = "CUENTA_ABONO";
        public const String Campo_Cuenta_Cargo = "CUENTA_CARGO";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Tipo_Gasto = "TIPO_GASTO";
        public const String Campo_Tipo_Matriz = "TIPO_MATRIZ";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Con_Tipo_Balance
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_CON_TIPO_BALANCE
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 06/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Tipo_Balance
    {
        public const String Tabla_Cat_Con_Tipo_Balance = "CAT_CON_TIPO_BALANCE";
        public const String Campo_Tipo_Balance_ID = "TIPO_BALANCE_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Tipo_Balance = "TIPO_BALANCE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Con_Niveles
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_CON_NIVELES
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 06/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Niveles
    {
        public const String Tabla_Cat_Con_Niveles = "CAT_CON_NIVELES";
        public const String Campo_Nivel_ID = "NIVEL_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Inicio_Nivel = "INICIO_NIVEL";
        public const String Campo_Final_Nivel = "FINAL_NIVEL";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Bitacora_Polizas
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_CON_BITACORA_POLIZAS
    /// PARAMETROS :
    /// CREO       : Salvador L. Rea Ayala
    /// FECHA_CREO : 24/Octubre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Bitacora_Polizas
    {
        public const String Tabla_Ope_Con_Bitacora_Polizas = "OPE_CON_BITACORA_POLIZAS";
        public const String Campo_No_Bitacora = "NO_BITACORA";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Debe = "DEBE";
        public const String Campo_Haber = "HABER";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Con_Tipo_Resultado
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_CON_TIPO_RESULTADO
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 06/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Tipo_Resultado
    {
        public const String Tabla_Cat_Con_Tipo_Resultado = "CAT_CON_TIPO_RESULTADO";
        public const String Campo_Tipo_Resultado_ID = "TIPO_RESULTADO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Con_Cuentas_Contables
    /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_CON_CUENTAS_CONTABLES
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 06/Junio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Cuentas_Contables
    {
        public const String Tabla_Cat_Con_Cuentas_Contables = "CAT_CON_CUENTAS_CONTABLES";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Tipo_Balance_ID = "TIPO_BALANCE_ID";
        public const String Campo_Nivel_ID = "NIVEL_ID";
        public const String Campo_Tipo_Resultado_ID = "TIPO_RESULTADO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Cuenta = "CUENTA";
        public const String Campo_Afectable = "AFECTABLE";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Tipo_Cuenta = "TIPO_CUENTA";
        public const String Campo_Tipo_Presupuestal = "TIPO_PRESUPUESTAL";
        public const string Campo_Cuenta_Contable_ID_Padre = "CUENTA_CONTABLE_ID_PADRE";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Polizas
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_CON_POLIZAS
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 10/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Polizas
    {
        public const String Tabla_Ope_Con_Polizas = "OPE_CON_POLIZAS";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Fecha_Poliza = "FECHA_POLIZA";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Total_Debe = "TOTAL_DEBE";
        public const String Campo_Total_Haber = "TOTAL_HABER";
        public const String Campo_No_Partidas = "NO_PARTIDAS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Empleado_ID_Creo = "EMPLEADO_ID_CREO";
        public const String Campo_Empleado_ID_Autorizo = "EMPLEADO_ID_AUTORIZO";
        public const String Campo_Referencia = "REFERENCIA";
        public const string Campo_Prefijo = "PREFIJO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Polizas_Detalles
    /// DESCRIPCION: Clase con contiene los datos de la tabla OPE_CON_POLIZAS_DETALLES
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 10/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Polizas_Detalles
    {
        public const String Tabla_Ope_Con_Polizas_Detalles = "OPE_CON_POLIZAS_DETALLES";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Partida = "PARTIDA";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Debe = "DEBE";
        public const String Campo_Haber = "HABER";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Consecutivo = "CONSECUTIVO";
        public const String Campo_Compromiso_ID = "COMPROMISO_ID";
        public const String Campo_Referencia = "REFERENCIA";
        public const String Campo_Beneficiario_ID = "BENEFICIARIO_ID";
        public const String Campo_Tipo_Beneficiario = "TIPO_BENEFICIARIO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Polizas_Detalles
    /// DESCRIPCION: Clase con contiene los datos de la tabla TEM_CON_POLIZAS_DETALLES
    /// PARAMETROS :
    /// CREO       : Yazmin A. Delgado Gómez
    /// FECHA_CREO : 10/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Tem_Con_Polizas_Detalles
    {
        public const String Tabla_Tem_Con_Polizas_Detalles = "TEM_CON_POLIZAS_DETALLES";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Partida = "PARTIDA";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Debe = "DEBE";
        public const String Campo_Haber = "HABER";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Consecutivo = "CONSECUTIVO";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Con_Parametros
    /// DESCRIPCION: Clase que contiene los datos de la tabla CAT_CON_PARAMETROS
    /// PARAMETROS :
    /// CREO       : Salvador L. Rea Ayala
    /// FECHA_CREO : 15/Septiembre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Parametros
    {
        public const String Tabla_Cat_Con_Parametros = "CAT_CON_PARAMETROS";
        public const String Campo_Parametro_Contabilidad_ID = "PARAMETRO_CONTABILIDAD_ID";
        public const String Campo_Mascara_Cuenta_Contable = "MASCARA_CUENTA_CONTABLE";
        public const String Campo_Mes_Contable = "MES_CONTABLE";
        public const String Campo_Usuario_Autoriza_Inv = "USUARIO_AUTORIZA_INV_PUBLIC";
        public const String Campo_Usuario_Autoriza_Inv_2 = "USUARIO_AUTORIZA_INV_PUBLIC2";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_IVA = "IVA";
        public const String Campo_Retencion_IVA = "RETENCION_IVA";
        public const String Campo_Cedular = "CEDULAR";
        public const String Campo_ISR = "ISR";
        public const String Campo_Cuenta_Almacen_General = "CUENTA_ALMACEN_GENERAL";
        public const String Campo_Cuenta_Almacen_Papeleria = "CUENTA_ALMACEN_PAPELERIA";
        public const String Campo_Tipo_Poliza_Diario_ID = "TIPO_POLIZA_DIARIO_ID";
        public const String Campo_Cuenta_IVA_Acreditable = "CUENTA_IVA_ACREDITABLE";
        public const String Campo_Tipo_Poliza_Egresos_ID = "TIPO_POLIZA_EGRESOS_ID";
        public const String Campo_Tipo_Poliza_Ingresos_ID = "TIPO_POLIZA_INGRESOS_ID";
        public const String Campo_Aplicar_Disponible_Acumulado = "APLICAR_DISPONIBLE_ACUMULADO";

        public const String Campo_CAP = "CAP";
        public const String Campo_RAPCE = "RAPCE";
        public const String Campo_DIVO = "DIVO";
        public const String Campo_SEFUPU = "SEFUPU";
        public const String Campo_ICIC = "ICIC";
        public const String Campo_OBS = "OBS";
        public const String Campo_Est_Y_Proy = "EST_Y_PROY";
        public const String Campo_Lab_Control_C = "LAB_CONTROL_C";
        public const String Campo_Iva_Inversiones = "IVA_INVERSIONES";
        public const String Campo_CTA_Retencion_IVA = "CTA_RETENCION_IVA";
        public const String Campo_CTA_Cedular = "CTA_CEDULAR";
        public const String Campo_CTA_ISR = "CTA_ISR";
        public const String Campo_CTA_CAP = "CTA_CAP";
        public const String Campo_CTA_RAPCE = "CTA_RAPCE";
        public const String Campo_CTA_DIVO = "CTA_DIVO";
        public const String Campo_CTA_SEFUPU = "CTA_SEFUPU";
        public const String Campo_CTA_ICIC = "CTA_ICIC";
        public const String Campo_CTA_OBS = "CTA_OBS";
        public const String Campo_CTA_Est_Y_Proy = "CTA_EST_Y_PROY";
        public const String Campo_CTA_Lab_Control_C = "CTA_LAB_CONTROL_C";
        public const String Campo_Fte_Financiamiento_ID_Ingresos = "FTE_FINANCIAMIENTO_ID_INGRESOS";
        public const String Campo_Fte_Financiamiento_ID_Ramo33 = "FTE_FINANCIAMIENTO_ID_RAMO33";
        public const String Campo_Programa_ID_Ingresos = "PROGRAMA_ID_INGRESOS";
        public const String Campo_Programa_ID_Ramo33 = "PROGRAMA_ID_RAMO33";
        public const String Campo_Devengado_Ing = "DEVENGADO_INGRESOS";
        public const String Campo_Obras_Proceso = "OBRAS_PROCESO_CUENTA_ID";
        public const String Campo_ISR_Arrendamiento = "ISR_ARRENDAMIENTO_ID";
        public const String Campo_Cedular_Arrendamiento = "CEDULAR_ARRENDAMIENTO_ID";
        public const String Campo_Honorarios_Asimilables = "HONORARIOS_ASIMILABLES_ID";
        public const String Campo_ISR_Arrendamiento_Monto = "ISR_ARRENDAMIENTO";
        public const String Campo_Cedular_Arrendamiento_Monto = "CEDULAR_ARRENDAMIENTO";
        public const String Campo_Honorarios_Asimilables_Monto = "HONORARIOS_ASIMILABLES";
        public const String Campo_CTA_Baja_Activo_ID = "CTA_BAJA_ACTIVO_ID";
        public const String Campo_Tipo_Poliza_Baja_ID = "TIPO_POLIZA_BAJA_ID";
        public const string Campo_Campo_Escondido_CONAC = "CAMPO_ESCONDIDO_CONAC";
        public const string Campo_Area_Funcional_ID_Poliza = "AREA_FUNCIONAL_ID_POLIZA";
        public const string Campo_CTA_Comision_Bancaria = "CTA_COMISION_BANCARIA";
        public const String Campo_Cta_Banco_Otros_Deudor = "CTA_BANCO_OTROS_DEUDOR";
        public const String Campo_Cta_Por_Pagar_Deudor = "CTA_POR_PAGAR_DEUDOR";
        public const String Campo_Tipo_Solicitud_Fondo_Revolvente = "TIPO_FONDO_REVOLVENTE_ID";
        public const String Campo_Tipo_Caja_Chica = "TIPO_CAJA_CHICA_ID";
        public const String Campo_Monto_Caja_Chica = "MONTO_CAJA_CHICA";
        public const String Campo_Deudor_Caja_Chica = "DEUDOR_CAJA_CHICA";
        public const String Campo_Usuario_Autoriza_Gastos = "USUARIO_AUTORIZA_GASTOS";
        public const String Campo_Dias_Comprobacion = "DIAS_COMPROBACION";
        public const String Campo_Cta_Banco_Deudor_GPC = "CTA_BANCO_DEUDOR_GPC";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Compromisos
    /// DESCRIPCION: Clase que contiene los datos de la tabla Ope_Con_Compromisos
    /// PARAMETROS :
    /// CREO       : Salvador L. Rea Ayala
    /// FECHA_CREO : 13/Octubre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Compromisos
    {
        public const String Tabla_Ope_Con_Compromisos = "OPE_CON_COMPROMISOS";
        public const String Campo_No_Compromiso = "NO_COMPROMISO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Monto_Comprometido = "MONTO_COMPROMETIDO";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Contratista_ID = "CONTRATISTA_ID";
        public const String Campo_Nombre_Beneficiario = "NOMBRE_BENEFICIARIO";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    /// NOMBRE DE LA FUNCION: Ope_Con_Cierre_Mensual
    /// DESCRIPCION: Clase que contiene los datos de la tabla OPE_CON_CIERRE_MENSUAL
    /// PARAMETROS :
    /// CREO       : Salvador L. Rea Ayala
    /// FECHA_CREO : 1/Octubre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Cierre_Mensual
    {
        public const String Tabla_Ope_Con_Cierre_Mensual = "OPE_CON_CIERRE_MENSUAL";
        public const String Campo_Cierre_Mensual_ID = "CIERRE_MENSUAL_ID";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Mes_Anio = "MES_ANIO";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Final = "FECHA_FINAL";
        public const String Campo_Saldo_Inicial = "SALDO_INICIAL";
        public const String Campo_Saldo_Final = "SALDO_FINAL";
        public const String Campo_Total_Debe = "TOTAL_DEBE";
        public const String Campo_Total_Haber = "TOTAL_HABER";
        public const String Campo_Diferencia = "DIFERENCIA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    /// NOMBRE DE LA FUNCION: Ope_Con_Cierre_Mensual_Gral
    /// DESCRIPCION: Clase que contiene los datos de la tabla OPE_CON_CIERRE_MENSUAL_GRAL
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 04/Noviembre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Cierre_Mensual_Gral
    {
        public const String Tabla_Ope_Con_Cierre_Mensual_Gral = "OPE_CON_CIERRE_MENSUAL_GRAL";
        public const String Campo_Cierre_Mensual_Gral_ID = "CIERRE_MENSUAL_GRAL_ID";
        public const String Campo_Mes = "MES";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Solicitud_Pagos
    /// DESCRIPCION: Clase que contiene los datos de la tabla OPE_CON_SOLICITUD_PAGOS
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 15/Noviembre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Solicitud_Pagos
    {
        public const String Tabla_Ope_Con_Solicitud_Pagos = "OPE_CON_SOLICITUD_PAGOS";
        public const String Campo_No_Solicitud_Pago = "NO_SOLICITUD_PAGO";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Tipo_Solicitud_Pago_ID = "TIPO_SOLICITUD_PAGO_ID";
        public const String Campo_Empleado_ID_Jefe_Area = "EMPLEADO_ID_JEFE_AREA";
        public const String Campo_Empleado_ID_Contabilidad = "EMPLEADO_ID_CONTABILIDAD";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Fecha_Solicitud = "FECHA_SOLICITUD";
        public const String Campo_Monto = "MONTO";
        public const String Campo_No_Factura = "NO_FACTURA";
        public const String Campo_Fecha_Factura = "FECHA_FACTURA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Fecha_Autorizo_Rechazo_Jefe = "FECHA_AUTORIZO_RECHAZO_JEFE";
        public const String Campo_Comentarios_Jefe_Area = "COMENTARIOS_JEFE_AREA";
        public const String Campo_Fecha_Autorizo_Rechazo_Contabilidad = "FECHA_AUTORIZO_RECHAZO_CONTABI";
        public const String Campo_Comentarios_Contabilidad = "COMENTARIOS_CONTABILIDAD";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Documentos = "DOCUMENTOS";
        public const String Campo_Fecha_Recepcion_Documentos = "FECHA_RECEPCION_DOCUMENTOS";
        public const String Campo_Usuario_Recibio = "USUARIO_RECIBIO";
        public const String Campo_Comentarios_Recepcion_Doc = "COMENTARIOS_RECEPCION_DOC";
        public const String Campo_Fecha_Autoriza_Rechaza_Ejercido = "FECHA_AUTORIZA_RECHAZA_EJD";
        public const String Campo_Comentario_Ejercido = "COMENTARIO_EJERCIDO";
        public const String Campo_Monto_Aprobado = "MONTO_COMPROBADO";
        public const String Campo_Fecha_Comprobado = "FECHA_COMPROBADO";
        public const String Campo_Usuario_Comprobo = "USUARIO_COMPROBO";
        public const String Campo_Transferencia = "TRANSFERENCIA";
        public const String Campo_Cuenta_Transferencia_ID = "CUENTA_TRANSFERENCIA_ID";
        public const String Campo_Usuario_Autorizo_Area = "USUARIO_AUTORIZO_AREA";
        public const String Campo_Usuario_Recibio_Doc_Fisica = "USUARIO_RECIBIO_DOC_FISICA";
        public const String Campo_Usuario_Autorizo_Documentos = "USUARIO_AUTORIZO_DOCUMENTOS";
        public const String Campo_Usuario_Recibio_Contabilidad = "USUARIO_RECIBIO_CONTABILIDAD";
        public const String Campo_Usuario_Autorizo_Contabilidad = "USUARIO_AUTORIZO_CONTABILIDAD";
        public const String Campo_Usuario_Recibio_Ejercido = "USUARIO_RECIBIO_EJERCIDO";
        public const String Campo_Usuario_Autorizo_Ejercido = "USUARIO_AUTORIZO_EJERCIDO";
        public const String Campo_Usuario_Recibio_Pagado = "USUARIO_RECIBIO_PAGADO";
        public const String Campo_Fecha_Recibio_Doc_Fisica = "FECHA_RECIBIO_DOC_FISICA";
        public const String Campo_Fecha_Recibio_Contabilidad = "FECHA_RECIBIO_CONTABILIDAD";
        public const String Campo_Fecha_Recibio_Ejercido = "FECHA_RECIBIO_EJERCIDO";
        public const String Campo_Fecha_Recibio_Pagado = "FECHA_RECIBIO_PAGADO";
        public const String Campo_Fecha_Layout_Transferencia = "FECHA_LAYOUT_TRANS";
        public const String Campo_Forma_Pago = "FORMA_PAGO";
        public const String Campo_Cuenta_Banco_Pago_ID = "CUENTA_BANCO_PAGO_ID";
        public const String Campo_No_Pago = "NO_PAGO";
        public const String Campo_Comentario_Finanzas = "COMENTARIOS_FINANZAS";
        public const String Campo_Servicios_Generales = "SERVICIOS_GENERALES";
        public const String Campo_Gasto_ID = "GASTO_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Con_Tipo_Solicitud_Pagos
    /// DESCRIPCION: Clase que contiene los datos de la tabla CAT_CON_TIPO_SOLICITUD_PAGO
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 15/Noviembre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Tipo_Solicitud_Pagos
    {
        public const String Tabla_Cat_Con_Tipo_Solicitud_Pago = "CAT_CON_TIPO_SOLICITUD_PAGOS";
        public const String Campo_Tipo_Solicitud_Pago_ID = "TIPO_SOLICITUD_PAGO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Descripcion_Finanzas = "DESCRIPCION_FINANZAS";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Tipo_Comprobacion = "TIPO_COMPROBACION";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Vales_Deudores
    /// DESCRIPCION: Clase que contiene los datos de la tabla OPE_CON_PAGOS
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 26/Agosto/2013
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Vales_Deudores
    {
        public const String Tabla_Ope_Con_Vales_Deudores = "OPE_CON_VALES_DEUDORES";
        public const String Campo_No_Vale_Deudor = "NO_VALE_DEUDOR";
        public const String Campo_No_Deuda = "NO_DEUDA";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }



    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Pagos
    /// DESCRIPCION: Clase que contiene los datos de la tabla OPE_CON_PAGOS
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 21/Noviembre/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Pagos
    {
        public const String Tabla_Ope_Con_Pagos = "OPE_CON_PAGOS";
        public const String Campo_No_Pago = "NO_PAGO";
        public const String Campo_No_Solicitud_Pago = "NO_SOLICITUD_PAGO";
        public const String Campo_No_poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Fecha_Pago = "FECHA_PAGO";
        public const String Campo_Banco_ID = "BANCO_ID";
        public const String Campo_Beneficiario_Pago = "BENEFICIARIO_PAGO";
        public const String Campo_Forma_Pago = "FORMA_PAGO";
        public const String Campo_Referencia_Transferencia_Banca = "REFERENCIA_TRANSFERENCIA_BANCA";
        public const String Campo_No_Cheque = "NO_CHEQUE";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_Motivo_Cancelacion = "MOTIVO_CANCELACION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Abono_Cuenta_Beneficiario = "ABONO_CUENTA_BENEFICIARIO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Con_Partida_Poliza_Modif
    /// DESCRIPCION: Clase con contiene los datos de la tabla Ope_Con_Partida_Poliza_Modif
    /// PARAMETROS :
    /// CREO       : Noe Mosqueda Valadez
    /// FECHA_CREO : 05/Junio/2013 10:11
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Partida_Poliza_Modif
    {
        public const string Tabla_Ope_Con_Partida_Poliza_Modificada = "Ope_Con_Partida_Poliza_Modif";
        public const string Campo_No_Partida_Modificada = "No_Partida_Modificada";
        public const string Campo_No_Poliza = "No_Poliza";
        public const string Campo_Tipo_Poliza_ID = "Tipo_Poliza_ID";
        public const string Campo_Mes_Anio = "Mes_Ano";
        public const string Campo_Cuenta_Contable_id = "Cuenta_Contable_ID";
        public const string Campo_Concepto = "Concepto";
        public const string Campo_Debe = "Debe";
        public const string Campo_Haber = "Haber";
        public const string Campo_Saldo = "Saldo";
        public const string Campo_Usuario_Creo = "Usuario_Creo";
        public const string Campo_Fecha_Creo = "Fecha_Creo";
        public const string Campo_Usuario_Modifico = "Usuario_Modifico";
        public const string Campo_Fecha_Modifico = "Fecha_Modifico";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: OPE_CON_SOLICITUD_PAGOS_DETALLES
    /// DESCRIPCION: Clase que contiene los datos de la tabla OPE_CON_SOLICITUD_PAGOS_DETALL
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 05/Enero/2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Con_Solicitud_Pagos_Detalles
    {
        public const String Tabla_Ope_Con_Solicitud_Pagos_Detalles = "OPE_CON_SOLICITUD_PAGOS_DETALL";
        public const String Campo_No_Solicitud_Pago = "NO_SOLICITUD_PAGO";
        public const String Campo_No_Factura = "NO_FACTURA";
        public const String Campo_Fecha_Factura = "FECHA_FACTURA";
        public const String Campo_Monto_Factura = "MONTO_FACTURA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Tipo_Documento = "TIPO_DOCUMENTO";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Ruta = "RUTA";
        public const String Campo_Archivo = "ARCHIVO";
        public const String Campo_Fte_Financimiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Iva = "IVA_FACTURA";
        public const String Campo_Deducciones = "DEDUCCIONES_FACTURA";
        public const String Campo_Nombre_Proveedor_Factura = "NOMBRE_PROVEEDOR_FACTURA";
        public const String Campo_Tipo_Operacion = "TIPO_OPERACION";
        public const String Campo_ISR = "ISR";
        public const String Campo_Retencion_IVA = "RETENCION_IVA";
        public const String Campo_Cedula = "CEDULA";
        public const String Campo_RFC = "RFC";
        public const String Campo_CURP = "CURP";
        public const String Campo_IEPS = "IEPS";
        public const String Campo_ISH = "ISH";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Com_Solicitud_Transf_Det
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla Ope_Com_Solicitud_Transf_Det
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 23/Noviembre/2011
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Com_Solicitud_Transf_Det
    {
        public const String Tabla_Ope_Psp_Hist_Calendar_Presu = "OPE_COM_SOLICITUD_TRANSF_DET";
        public const String Campo_No_Solicitud_Detalle = "NO_SOLICITUD_DETALLE";
        public const String Campo_No_Solicitud = "NO_SOLICITUD";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Tipo = "TIPO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Reservas_Detalles
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla Ope_Psp_Reservas_Detalles
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 20/ENERO/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Reservas_Detalles
    {
        public const String Tabla_Ope_Psp_Reservas_Detalles = "OPE_PSP_RESERVAS_DETALLES";
        public const String Campo_No_Reserva_Detalle = "NO_RESERVA_DETALLE";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Importe_Inicial = "IMPORTE_INICIAL";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Fte_Financimiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Con_Contratos_Det
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla Cat_Con_Contratos_Det
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 23/Noviembre/2011
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Con_Contratos_Det
    {
        public const String Tabla_Cat_Con_Contratos_Det = "CAT_CON_CONTRATOS_DET";
        public const String Campo_Contrato_ID = "CONTRATO_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Con_Contratos
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla Cat_Con_Contratos
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 23/Noviembre/2011
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Con_Contratos
    {
        public const String Tabla_Cat_Con_Contratos = "CAT_CON_CONTRATOS";
        public const String Campo_Contrato_ID = "CONTRATO_ID";
        public const String Campo_Numero_Contrato = "NUMERO_CONTRATO";
        public const String Campo_Nombre_Contrato = "NOMBRE_CONTRATO";
        public const String Campo_Fecha_Inicio = "FECHA_INICIO";
        public const String Campo_Fecha_Fin = "FECHA_FIN";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Anticipo_Monto = "ANTICIPO_MONTO";
        public const String Campo_Anticipo_Porcentaje = "ANTICIPO_PORCENTAJE";
        public const String Campo_Amortizado = "AMORTIZADO";
        public const String Campo_Por_Amortizar = "POR_AMORTIZAR";
        public const String Campo_Saldo_Total = "SALDO_TOTAL";
        public const String Campo_Avance_Fisico = "AVANCE_FISICO";
        public const String Campo_Avance_Financiero = "AVANCE_FINANCIERO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_Convenio_ID = "CONVENIO_ID";
        public const String Campo_Fecha_Inicial_Original = "FECHA_INICIAL_ORIGINAL";
        public const String Campo_Fecha_Final_Original = "FECHA_FINAL_ORIGINAL";
        public const String Campo_Importe_Original = "IMPORTE_ORIGINAL";
        public const String Campo_Motivo = "MOTIVO";
        public const String Campo_Archivo = "ARCHIVO";
        public const String Campo_Ruta_Archivo = "RUTA_ARCHIVO";
        public const String Campo_Cuenta_Banco_Contratos_ID = "CUENTA_BANCO_CONTRATO_ID";
        public const String Campo_Forma_Pago = "FORMA_PAGO";
        public const String Campo_Localidad = "LOCALIDAD_ID";
        public const String Campo_Modalidad = "MODALIDAD";
        public const String Campo_Fianza_Garantia = "FIANZA_GARANTIA";
        public const String Campo_Fianza_Anticipo = "FIANZA_ANTICIPO";
        public const String Campo_Fecha_Fianza = "FECHA_FIANZA";
        public const String Campo_Monto_Garantia = "MONTO_GARANTIA";
        public const String Campo_Monto_Anticipo = "MONTO_ANTICIPO";
        public const String Campo_Fecha_Finiquito = "FECHA_FINIQUITO";
    }
    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Con_Contratos_Ampliaciones
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla Cat_Con_Contratos_Ampliaciones
    ///PARAMETROS           :
    ///CREO                 : Sergio Manuel Gallardo Andrade
    ///FECHA_CREO           : 26/Septiembre/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Con_Contratos_Ampliaciones
    {
        public const String Tabla_Cat_Con_Contratos_Ampliaciones = "CAT_CON_CONTRATOS_AMPLIACION";
        public const String Campo_Contrato_ID = "CONTRATO_ID";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Fecha_Amp_Inicio = "FECHA_AMP_INICIO";
        public const String Campo_Fecha_Amp_Fin = "FECHA_AMP_FINAL";
        public const String Campo_Motivo = "MOTIVO";
        public const String Campo_Archivo = "ARCHIVO";
        public const String Campo_Ruta_Archivo = "RUTA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Con_Contratos_Acciones
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla Cat_Con_Contratos_Acciones
    ///PARAMETROS           :
    ///CREO                 : Sergio manuel Gallardo Andrade
    ///FECHA_CREO           : 17/Septiembre/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Con_Contratos_Acciones
    {
        public const String Tabla_Cat_Con_Contratos_Acciones = "CAT_CON_CONTRATOS_ACCIONES";
        public const String Campo_Contrato_ID = "CONTRATO_ID";
        public const String Campo_Contrato_Accion_ID = "CONTRATO_ACCION_ID";
        public const String Campo_Accion_ID = "ACCION_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: CAT_CON_ACCIONES
    /// DESCRIPCION: Clase que contiene los datos de la tabla CAT_CON_ACCIONES
    /// PARAMETROS :
    /// CREO       : Armando Zavala Moreno
    /// FECHA_CREO : 09/Agosto/2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Acciones
    {
        public const String Tabla_Cat_Con_Acciones = "CAT_CON_ACCIONES";
        public const String Campo_Accion_Id = "ACCION_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: CAT_CON_CONVENIOS_ACCIONES
    /// DESCRIPCION: Clase que contiene los datos de la tabla CAT_CON_CONVENIOS_ACCIONES
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 11/Septiembre/2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Convenios_Acciones
    {
        public const String Tabla_Cat_Con_Convenios_Acciones = "CAT_CON_CONVENIOS_ACCIONES";
        public const String Campo_Accion_ID = "ACCION_ID";
        public const String Campo_Convenio_Accion_ID = "CONVENIO_ACCION_ID";
        public const String Campo_Convenio_ID = "CONVENIO_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: CAT_CON_PROGRAMAS_ACCIONES
    /// DESCRIPCION: Clase que contiene los datos de la tabla CAT_CON_PROGRAMAS_ACCIONES
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 11/Septiembre/2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Programas_Acciones
    {
        public const String Tabla_Cat_Con_Programas_Acciones = "CAT_CON_PROGRAMAS_ACCIONES";
        public const String Campo_Accion_ID = "ACCION_ID";
        public const String Campo_Programa_Accion_ID = "PROGRAMA_ACCION_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Convenio_ID = "CONVENIO_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: OPE_CON_DEUDORES
    /// DESCRIPCION: Clase que contiene los datos de la tabla OPE_CON_DEUDORES
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo Andrade
    /// FECHA_CREO : 03/Agosto/2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class OPE_CON_DEUDORES
    {
        public const String Tabla_Ope_Con_Deudores = "OPE_CON_DEUDORES";
        public const String Campo_No_Deuda = "NO_DEUDA";
        public const String Campo_Deudor_ID = "DEUDOR_ID";
        public const String Campo_Tipo_Deudor = "TIPO_DEUDOR";
        public const String Campo_Tipo_Movimiento = "TIPO_MOVIMIENTO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Fecha_Limite = "FECHA_LIMITE";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Dependencia_ID = "DEP_USUARIO_CREO_ID";
        public const String Campo_Fecha_Autorizo_Rechazo = "FECHA_AUTORIZO_RECHAZO_JEFE";
        public const String Campo_Usuario_Autorizo_Area = "USUARIO_AUTORIZO_AREA";
        public const String Campo_Comentario_Area = "COMENTARIO_AREA";
        public const String Campo_Forma_Pago = "FORMA_PAGO";
        public const String Campo_Cuenta_Banco_Pago = "CUENTA_BANCO_PAGO";
        public const String Campo_Comentario_Finanzas = "COMENTARIO_FINANZA";
        public const String Campo_Fecha_Autoriza_Rechaza_Finanzas = "FECHA_AUTORIZO_RECHAZO_FIN";
        public const String Campo_Usuario_Asigna_Cuenta = "USUARIO_ASIGNO_BANCO";
        public const String Campo_No_Pago = "NO_PAGO";
        public const String Campo_Transferencia = "TRANSFERENCIA";
        public const String Campo_Cuenta_Transferencia_ID = "CUENTA_TRANSFERENCIA_ID";
        public const String Campo_Fecha_Layout_Transferencia = "FECHA_LAYOUT_TRANS";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Contrato_ID = "CONTRATO_ID";
        public const String Campo_No_Solicitud_Inversiones = "NO_SOLICITUD_INVERSIONES";
        public const String Campo_Tipo_Solicitud_ID = "TIPO_SOLICITUD_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: CAT_CON_CUENTAS_GASTOS
    /// DESCRIPCION: Clase que contiene los datos de la tabla CAT_CON_CUENTAS_GASTOS
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo
    /// FECHA_CREO : 09/Agosto/2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Cuentas_Gastos
    {
        public const String Tabla_Cat_Con_Cuentas_Gastos = "CAT_CON_CUENTAS_GASTOS";
        public const String Campo_Id_Gasto = "CUENTA_GASTO_ID";
        public const String Campo_Cuenta_Contable_Id = "CUENTA_CONTABLE_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Partida_ID = "PARTIDA_ID";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Con_Cuentas_Gastos_Detalles
    /// DESCRIPCION: Clase que contiene los datos de la tabla CAT_CON_CUENTAS_GASTOS_DETALLES
    /// PARAMETROS :
    /// CREO       : Sergio Manuel Gallardo
    /// FECHA_CREO : 09/Agosto/2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Con_Cuentas_Gastos_Detalles
    {
        public const String Tabla_Cat_Con_Cuentas_Gastos_Detalles = "CAT_CON_CUENTAS_GASTOS_DETALLES";
        public const String Campo_Id_Gasto = "CUENTA_GASTO_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    #endregion


    ///*************************************************************************************************************************
    ///                                                                VARIOS
    ///*************************************************************************************************************************
    #region VARIOS
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Cuentas
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_CUENTAS
    /// PARÁMETROS :     
    /// CREO       :
    /// FECHA_CREO : 20/Agosto/2010 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Cuentas
    {
        public const String Tabla_Cat_Cuentas = "CAT_CUENTAS";
        public const String Campo_Cuenta_ID = "CUENTA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_No_Cuenta = "NUMERO_CUENTA";
        public const String Campo_Banco = "BANCO";
        public const String Campo_Comentarios = "COMENTARIOS";
        public const String Campo_UsuarioCreo = "USUARIO_CREO";
        public const String Campo_FechaCreo = "FECHA_CREO";
        public const String Campo_UsuarioModifico = "USUARIO_MODIFICO";
        public const String Campo_FechaModifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Cat_Con_Cuentas_Fijas
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_CON_CUENTAS_FIJAS
    /// PARÁMETROS :     
    /// CREO       : Gustavo Angeles
    /// FECHA_CREO : 11/Julio/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Con_Cuentas_Fijas
    {
        public const String Tabla_Cat_Con_Cuentas_Fijas = "CAT_CON_CUENTAS_FIJAS";
        public const String Campo_Almacen_General = "ALMACEN_GENERAL";
        public const String Campo_Iva_Acreditable_Compras = "IVA_ACREDITABLE_COMPRAS";
    }
    public class Apl_Cat_Preg_Resp
    {
        public const String Tabla_Apl_Cat_Preg_Resp = "APL_CAT_PREG_RESP";
        public const String Campo_Preg_Resp_ID = "PREG_RESP_ID";
        public const String Campo_Pregunta = "PREGUNTA";
        public const String Campo_Respuesta = "RESPUESTA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Cat_Det_Empleado_UR
    /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_DET_EMPLEADO_UR 
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armeta
    /// FECHA_CREO :           01/Sep/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************/
    public class Cat_Det_Empleado_UR
    {
        public const String Tabla_Cat_Det_Empleado_UR = "CAT_DET_EMPLEADO_UR";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";

    }

    #endregion

    #region INGRESOS


    ///*******************************************************************************
    ///NOMBRE DE LA CLASE: Cat_Pre_Claves_Ingreso
    ///DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PRE_CLAVES_INGRESO
    ///PARÁMETROS :     
    ///CREO       : José Alfredo García Pichardo
    ///FECHA_CREO : 21/Julio/2011 
    ///MODIFICO          :
    ///FECHA_MODIFICO    :
    ///CAUSA_MODIFICACIÓN:
    ///*********************************************************************************/
    //public class Cat_Pre_Claves_Ingreso
    //{
    //    public const String Tabla_Cat_Pre_Claves_Igreso = "CAT_PRE_CLAVES_INGRESO";
    //    public const String Campo_Clave_Ingreso_ID = "CLAVE_INGRESO_ID";
    //    public const String Campo_Grupo_ID = "GRUPO_ID";
    //    public const String Campo_Estatus = "ESTATUS";
    //    public const String Campo_Clave = "CLAVE";
    //    public const String Campo_Descripcion = "DESCRIPCION";
    //    public const String Campo_Fundamento = "FUNDAMENTO";
    //    public const String Campo_Usuario_Creo = "USUARIO_CREO";
    //    public const String Campo_Fecha_Creo = "FECHA_CREO";
    //    public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
    //    public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    //}
    #endregion

    #region PRESUPUESTOS

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Registro_Movimiento_Detalle
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Registro_Movimiento_Detalle
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 25/Septiembre/2013
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Registro_Movimiento_Detalle
    {
        public const String Tabla_Ope_Psp_Registro_Movimiento_Detalle = "OPE_PSP_REGISTRO_MOVIMIENTO_DETALLE";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_Cargo = "CARGO";
        public const String Campo_Abono = "ABONO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Anio = "MES_ANIO";
        public const String Campo_No_Poliza_Psp = "NO_POLIZA_PSP";
        public const String Campo_Tipo_Poliza_ID_Psp = "TIPO_POLIZA_ID_PSP";
        public const String Campo_Mes_Anio_Psp = "MES_ANIO_PSP";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Registro_Afectacion_Presupuesto
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Registro_Afectacion_Presupuesto
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 19/Septiembre/2013
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Registro_Afectacion_Presupuesto
    {
        public const String Tabla_Ope_Psp_Registro_Afectacion_Presupuesto = "OPE_PSP_REGISTRO_AFECTACION_PRESUPUESTO";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_Cargo = "CARGO";
        public const String Campo_Abono = "ABONO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Anio = "MES_ANIO";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Psp_SubConcepto_Ing
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Cat_Psp_SubConcepto_Ing
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           :  07/Mayo/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_SubConcepto_Ing
    {
        public const String Tabla_Cat_Psp_SubConcepto_Ing = "CAT_PSP_SUBCONCEPTO_ING";
        public const String Campo_SubConcepto_Ing_ID = "SUBCONCEPTO_ING_ID";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Lleva_IVA = "LLEVA_IVA";
        public const String Campo_Tipo_Calculo = "TIPO_CALCULO";
        public const String Campo_Cantidad_Calculo = "CANTIDAD_CALCULO";
        public const String Campo_Impuesto_ID = "IMPUESTO_ID";
        public const String Campo_Concepto_Categoria_ID = "CONCEPTO_CATEGORIA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Psp_Parametros_Presupuestales
    ///DESCRIPCIÓN          : Clase contiene los datos de los parametros de presupuestos
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 27/Marzo/2013
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_Parametros_Presupuestales
    {
        public const String Tabla_Cat_Psp_Parametro_Presupuestal = "CAT_PSP_PARAMETRO_PRESUPUESTAL";
        public const String Campo_Fte_Financiamiento_ID_Ingresos = "FTE_FINANCIAMIENTO_ID_INGRESOS";
        public const String Campo_Programa_ID_Ingresos = "PROGRAMA_ID_INGRESOS";
        public const String Campo_Fte_Financiamiento_ID_Inversiones = "FTE_FINANCIAMIENTO_ID_INVERSIONES";
        public const String Campo_Programa_ID_Inversiones = "PROGRAMA_ID_INVERSIONES";
        public const String Campo_Cuenta_Orden_Ingresos_Estimado = "CTA_ORDEN_ING_ESTIMADO";
        public const String Campo_Cuenta_Orden_Ingresos_Ampliacion = "CTA_ORDEN_ING_AMPLIACION";
        public const String Campo_Cuenta_Orden_Ingresos_Reduccion = "CTA_ORDEN_ING_REDUCCION";
        public const String Campo_Cuenta_Orden_Ingresos_Modificado = "CTA_ORDEN_ING_MODIFICADO";
        public const String Campo_Cuenta_Orden_Ingresos_Por_Recaudar = "CTA_ORDEN_ING_POR_RECAUDAR";
        public const String Campo_Cuenta_Orden_Ingresos_Devengado = "CTA_ORDEN_ING_DEVENGADO";
        public const String Campo_Cuenta_Orden_Ingresos_Recaudado = "CTA_ORDEN_ING_RECAUDADO";
        public const String Campo_Cuenta_Orden_Egresos_Aprobado = "CTA_ORDEN_EGR_APROBADO";
        public const String Campo_Cuenta_Orden_Egresos_Ampliacion = "CTA_ORDEN_EGR_AMPLIACION";
        public const String Campo_Cuenta_Orden_Egresos_Reduccion = "CTA_ORDEN_EGR_REDUCCION";
        public const String Campo_Cuenta_Orden_Egresos_Ampliacion_Interna = "CTA_ORDEN_EGR_AMPLIACION_INTERNA";
        public const String Campo_Cuenta_Orden_Egresos_Reduccion_Interna = "CTA_ORDEN_EGR_REDUCCION_INTERNA";
        public const String Campo_Cuenta_Orden_Egresos_Modificado = "CTA_ORDEN_EGR_MODIFICADO";
        public const String Campo_Cuenta_Orden_Egresos_Por_Ejercer = "CTA_ORDEN_EGR_POR_EJERCER";
        public const String Campo_Cuenta_Orden_Egresos_Comprometido = "CTA_ORDEN_EGR_COMPROMETIDO";
        public const String Campo_Cuenta_Orden_Egresos_Devengado = "CTA_ORDEN_EGR_DEVENGADO";
        public const String Campo_Cuenta_Orden_Egresos_Ejercido = "CTA_ORDEN_EGR_EJERCIDO";
        public const String Campo_Cuenta_Orden_Egresos_Pagado = "CTA_ORDEN_EGR_PAGADO";
        public const String Campo_Aplicar_Disponible_Acumulado = "APLICAR_DISPONIBLE_ACUMULADO";
        public const String Campo_Tipo_Poliza_ID_Psp_Egr = "TIPO_POLIZA_ID_PSP_EGR";
        public const String Campo_Tipo_Poliza_ID_Psp_Ing = "TIPO_POLIZA_ID_PSP_ING";
        public const String Campo_Clasificador_Adm_ID = "CLASIFICADOR_ADM_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Psp_Clasificador_Administrativo
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla CAT_PSP_CLASIFICADOR_ADMINISTRATIVO
    ///PARAMETROS           :
    ///CREO                 : David Herrera Rincon
    ///FECHA_CREO           : 18/Febrero/2013
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_Clasificador_Administrativo
    {
        public const String Tabla_Cat_Psp_Clasificador_Administrativo = "CAT_PSP_CLASIFICADOR_ADMINISTRATIVO";
        public const String Campo_CADM_ID = "CLASIFICADOR_ADM_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Psp_Clasificador_Economico
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla CAT_PSP_CLASIFICADOR_ECONOMICO
    ///PARAMETROS           :
    ///CREO                 : David Herrera Rincon
    ///FECHA_CREO           : 12/Febrero/2013
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_Clasificador_Economico
    {
        public const String Tabla_Cat_Psp_Clasificador_Economico = "CAT_PSP_CLASIFICADOR_ECONOMICO";
        public const String Campo_CE_ID = "CE_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Psp_Clasificador_Tipo_Gasto
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla CAT_PSP_CLASIFICAdor_TIPO_GASTO
    ///PARAMETROS           :
    ///CREO                 : David Herrera Rincon
    ///FECHA_CREO           : 12/Febrero/2013
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_Clasificador_Tipo_Gasto
    {
        public const String Tabla_Cat_Psp_Clasificador_Tipo_Gasto = "CAT_PSP_CLASIFICADOR_TIPO_GASTO";
        public const String Campo_CTG_ID = "CTG_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///*******************************************************************************
    ///NOMBRE DE LA CLASE   : Cat_Ing_Procesos
    ///DESCRIPCIÓN          : Clase que contiene los campos de la tabla CAT_ING_PROCESOS
    ///PARÁMETROS:
    ///CREO                 : Antonio Salvador Benavides Guaradado
    ///FECHA_CREO           : 30/Junio/2012
    ///MODIFICO:
    ///FECHA_MODIFICO:
    ///CAUSA_MODIFICACIÓN:
    ///*********************************************************************************/
    public class Cat_Ing_Procesos
    {
        public const String Tabla_Cat_Ing_Procesos = "CAT_ING_PROCESOS";
        public const String Campo_Proceso_ID = "PROCESO_ID";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Cat_Organigrama
    /// DESCRIPCION: Clase con contiene los datos de la tabla Cat_Organigrama
    /// PARAMETROS :
    /// CREO       : Ramón Baeza Yépez
    /// FECHA_CREO : 26/Junio/2012
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Organigrama
    {
        public const String Tabla_Cat_Organigrama = "CAT_ORGANIGRAMA";
        public const String Campo_Parametro_ID = "PARAMETRO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Tipo = "TIPO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : CAT_PSP_RUBRO
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla CAT_PSP_RUBRO
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 15/Marzo/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_Rubro
    {
        public const String Tabla_Cat_Psp_Rubro = "CAT_PSP_RUBRO";
        public const String Campo_Rubro_ID = "RUBRO_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : CAT_PSP_TIPO
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla CAT_PSP_TIPO
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 15/Marzo/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_Tipo
    {
        public const String Tabla_Cat_Psp_Tipo = "CAT_PSP_TIPO";
        public const String Campo_Tipo_ID = "TIPO_ID";
        public const String Campo_Rubro_ID = "RUBRO_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Movimiento_Ing
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Movimiento_Ing
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 09/MAYO/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Movimiento_Ing
    {
        public const String Tabla_Ope_Psp_Movimiento_Ing = "OPE_PSP_MOVIMIENTO_ING";
        public const String Campo_No_Movimiento_Ing = "NO_MOVIMIENTO_ING";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Total_Modificado = "TOTAL_MODIFICADO";
        public const String Campo_Total_Modificado_Egr = "TOTAL_MODIFICADO_EGR";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Estatus_Ingresos = "ESTATUS_INGRESOS";
        public const String Campo_Estatus_Egr_Municipal = "ESTATUS_EGR_MUNICIPAL";
        public const String Campo_Estatus_Egr_Ramo33 = "ESTATUS_EGR_RAMO33";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Psp_Pronostico_Ingresos
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Cat_Psp_Pronostico_Ingresos
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 21/Marzo/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Pronostico_Ingresos
    {
        public const String Tabla_Ope_Psp_Pronostico_Ingresos = "OPE_PSP_PRONOSTICO_INGRESOS";
        public const String Campo_Pronostico_Ing_ID = "PRONOSTICO_ING_ID";
        public const String Campo_Rubro_ID = "RUBRO_ID";
        public const String Campo_Tipo_ID = "TIPO_ID";
        public const String Campo_Clase_Ing_ID = "CLASE_ING_ID";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
        public const String Campo_SubConcepto_Ing_ID = "SUBCONCEPTO_ING_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Poliza_Presupuestal = "NO_POLIZA_PRESUPUESTAL ";
        public const String Campo_Tipo_Poliza_ID_Presupuestal = "TIPO_POLIZA_ID_PRESUPUESTAL";
        public const String Campo_Mes_Anio_Presupuestal = "MES_ANIO_PRESUPUESTAL";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Psp_Clase_Ing
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Cat_Psp_Clase_Ing
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 20/Marzo/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_Clase_Ing
    {
        public const String Tabla_Cat_Psp_Clase_Ing = "CAT_PSP_CLASE_ING";
        public const String Campo_Clase_Ing_ID = "CLASE_ING_ID";
        public const String Campo_Tipo_ID = "TIPO_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }




    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Sap_Det_Fte_Programa
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Cat_Sap_Det_Fte_Programa
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 22/Junio/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Sap_Det_Fte_Programa
    {
        public const String Tabla_Cat_Sap_Det_Fte_Programa = "CAT_SAP_DET_FTE_PROGRAMA";
        public const String Campo_Fuente_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Importe_Calendarizado = "IMPORTE_CALENDARIZADO";
        public const String Campo_Importe_Modificado = "IMPORTE_MODIFICADO";
        public const String Campo_Porcentaje = "PORCENTAJE";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Presupuesto_Ingresos
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Presupuesto_Ingresos
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 12/Abril/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Presupuesto_Ingresos
    {
        public const String Tabla_Ope_Psp_Presupuesto_Ingresos = "OPE_PSP_PRESUPUESTO_INGRESOS";
        public const String Campo_Presupuesto_Ing_ID = "PRESUPUESTO_ING_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Rubro_ID = "RUBRO_ID";
        public const String Campo_Tipo_ID = "TIPO_ID";
        public const String Campo_Clase_Ing_ID = "CLASE_ING_ID";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
        public const String Campo_SubConcepto_Ing_ID = "SUBCONCEPTO_ING_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Aprobado = "APROBADO";
        public const String Campo_Ampliacion = "AMPLIACION";
        public const String Campo_Reduccion = "REDUCCION";
        public const String Campo_Modificado = "MODIFICADO";
        public const String Campo_Devengado = "DEVENGADO";
        public const String Campo_Recaudado = "RECAUDADO";
        public const String Campo_Devengado_Recaudado = "DEVENGADO_RECAUDADO";
        public const String Campo_Compromiso = "COMPROMISO";
        public const String Campo_Por_Recaudar = "POR_RECAUDAR";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Acumulado_Enero = "ACUMULADO_ENERO";
        public const String Campo_Acumulado_Febrero = "ACUMULADO_FEBRERO";
        public const String Campo_Acumulado_Marzo = "ACUMULADO_MARZO";
        public const String Campo_Acumulado_Abril = "ACUMULADO_ABRIL";
        public const String Campo_Acumulado_Mayo = "ACUMULADO_MAYO";
        public const String Campo_Acumulado_Junio = "ACUMULADO_JUNIO";
        public const String Campo_Acumulado_Julio = "ACUMULADO_JULIO";
        public const String Campo_Acumulado_Agosto = "ACUMULADO_AGOSTO";
        public const String Campo_Acumulado_Septiembre = "ACUMULADO_SEPTIEMBRE";
        public const String Campo_Acumulado_Octubre = "ACUMULADO_OCTUBRE";
        public const String Campo_Acumulado_Noviembre = "ACUMULADO_NOVIEMBRE";
        public const String Campo_Acumulado_Diciembre = "ACUMULADO_DICIEMBRE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Por_Recaudar_Enero = "POR_RECAUDAR_ENERO";
        public const String Campo_Por_Recaudar_Febrero = "POR_RECAUDAR_FEBRERO";
        public const String Campo_Por_Recaudar_Marzo = "POR_RECAUDAR_MARZO";
        public const String Campo_Por_Recaudar_Abril = "POR_RECAUDAR_ABRIL";
        public const String Campo_Por_Recaudar_Mayo = "POR_RECAUDAR_MAYO";
        public const String Campo_Por_Recaudar_Junio = "POR_RECAUDAR_JUNIO";
        public const String Campo_Por_Recaudar_Julio = "POR_RECAUDAR_JULIO";
        public const String Campo_Por_Recaudar_Agosto = "POR_RECAUDAR_AGOSTO";
        public const String Campo_Por_Recaudar_Septiembre = "POR_RECAUDAR_SEPTIEMBRE";
        public const String Campo_Por_Recaudar_Octubre = "POR_RECAUDAR_OCTUBRE";
        public const String Campo_Por_Recaudar_Noviembre = "POR_RECAUDAR_NOVIEMBRE";
        public const String Campo_Por_Recaudar_Diciembre = "POR_RECAUDAR_DICIEMBRE";
        public const String Campo_Devengado_Enero = "DEVENGADO_ENERO";
        public const String Campo_Devengado_Febrero = "DEVENGADO_FEBRERO";
        public const String Campo_Devengado_Marzo = "DEVENGADO_MARZO";
        public const String Campo_Devengado_Abril = "DEVENGADO_ABRIL";
        public const String Campo_Devengado_Mayo = "DEVENGADO_MAYO";
        public const String Campo_Devengado_Junio = "DEVENGADO_JUNIO";
        public const String Campo_Devengado_Julio = "DEVENGADO_JULIO";
        public const String Campo_Devengado_Agosto = "DEVENGADO_AGOSTO";
        public const String Campo_Devengado_Septiembre = "DEVENGADO_SEPTIEMBRE";
        public const String Campo_Devengado_Octubre = "DEVENGADO_OCTUBRE";
        public const String Campo_Devengado_Noviembre = "DEVENGADO_NOVIEMBRE";
        public const String Campo_Devengado_Diciembre = "DEVENGADO_DICIEMBRE";
        public const String Campo_Recaudado_Enero = "RECAUDADO_ENERO";
        public const String Campo_Recaudado_Febrero = "RECAUDADO_FEBRERO";
        public const String Campo_Recaudado_Marzo = "RECAUDADO_MARZO";
        public const String Campo_Recaudado_Abril = "RECAUDADO_ABRIL";
        public const String Campo_Recaudado_Mayo = "RECAUDADO_MAYO";
        public const String Campo_Recaudado_Junio = "RECAUDADO_JUNIO";
        public const String Campo_Recaudado_Julio = "RECAUDADO_JULIO";
        public const String Campo_Recaudado_Agosto = "RECAUDADO_AGOSTO";
        public const String Campo_Recaudado_Septiembre = "RECAUDADO_SEPTIEMBRE";
        public const String Campo_Recaudado_Octubre = "RECAUDADO_OCTUBRE";
        public const String Campo_Recaudado_Noviembre = "RECAUDADO_NOVIEMBRE";
        public const String Campo_Recaudado_Diciembre = "RECAUDADO_DICIEMBRE";
        public const String Campo_Actualizado = "ACTUALIZADO";
    }


    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Presupuesto_Ing_Esp
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Presupuesto_Ing_Esp
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 09/Mayo/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Presupuesto_Ing_Esp
    {
        public const String Tabla_Ope_Psp_Presupuesto_Ing_Esp = "OPE_PSP_PRESUPUESTO_ING_ESP";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Rubro_ID = "RUBRO_ID";
        public const String Campo_Tipo_ID = "TIPO_ID";
        public const String Campo_Clase_Ing_ID = "CLASE_ING_ID";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
        public const String Campo_SubConcepto_Ing_ID = "SUBCONCEPTO_ING_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_No_Modificacion = "NO_MODIFICACION";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Aprobado = "APROBADO";
        public const String Campo_Ampliacion = "AMPLIACION";
        public const String Campo_Reduccion = "REDUCCION";
        public const String Campo_Modificado = "MODIFICADO";
        public const String Campo_Devengado = "DEVENGADO";
        public const String Campo_Recaudado = "RECAUDADO";
        public const String Campo_Devengado_Recaudado = "DEVENGADO_RECAUDADO";
        public const String Campo_Compromiso = "COMPROMISO";
        public const String Campo_Por_Recaudar = "POR_RECAUDAR";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Por_Recaudar_Enero = "POR_RECAUDAR_ENERO";
        public const String Campo_Por_Recaudar_Febrero = "POR_RECAUDAR_FEBRERO";
        public const String Campo_Por_Recaudar_Marzo = "POR_RECAUDAR_MARZO";
        public const String Campo_Por_Recaudar_Abril = "POR_RECAUDAR_ABRIL";
        public const String Campo_Por_Recaudar_Mayo = "POR_RECAUDAR_MAYO";
        public const String Campo_Por_Recaudar_Junio = "POR_RECAUDAR_JUNIO";
        public const String Campo_Por_Recaudar_Julio = "POR_RECAUDAR_JULIO";
        public const String Campo_Por_Recaudar_Agosto = "POR_RECAUDAR_AGOSTO";
        public const String Campo_Por_Recaudar_Septiembre = "POR_RECAUDAR_SEPTIEMBRE";
        public const String Campo_Por_Recaudar_Octubre = "POR_RECAUDAR_OCTUBRE";
        public const String Campo_Por_Recaudar_Noviembre = "POR_RECAUDAR_NOVIEMBRE";
        public const String Campo_Por_Recaudar_Diciembre = "POR_RECAUDAR_DICIEMBRE";
        public const String Campo_Devengado_Enero = "DEVENGADO_ENERO";
        public const String Campo_Devengado_Febrero = "DEVENGADO_FEBRERO";
        public const String Campo_Devengado_Marzo = "DEVENGADO_MARZO";
        public const String Campo_Devengado_Abril = "DEVENGADO_ABRIL";
        public const String Campo_Devengado_Mayo = "DEVENGADO_MAYO";
        public const String Campo_Devengado_Junio = "DEVENGADO_JUNIO";
        public const String Campo_Devengado_Julio = "DEVENGADO_JULIO";
        public const String Campo_Devengado_Agosto = "DEVENGADO_AGOSTO";
        public const String Campo_Devengado_Septiembre = "DEVENGADO_SEPTIEMBRE";
        public const String Campo_Devengado_Octubre = "DEVENGADO_OCTUBRE";
        public const String Campo_Devengado_Noviembre = "DEVENGADO_NOVIEMBRE";
        public const String Campo_Devengado_Diciembre = "DEVENGADO_DICIEMBRE";
        public const String Campo_Recaudado_Enero = "RECAUDADO_ENERO";
        public const String Campo_Recaudado_Febrero = "RECAUDADO_FEBRERO";
        public const String Campo_Recaudado_Marzo = "RECAUDADO_MARZO";
        public const String Campo_Recaudado_Abril = "RECAUDADO_ABRIL";
        public const String Campo_Recaudado_Mayo = "RECAUDADO_MAYO";
        public const String Campo_Recaudado_Junio = "RECAUDADO_JUNIO";
        public const String Campo_Recaudado_Julio = "RECAUDADO_JULIO";
        public const String Campo_Recaudado_Agosto = "RECAUDADO_AGOSTO";
        public const String Campo_Recaudado_Septiembre = "RECAUDADO_SEPTIEMBRE";
        public const String Campo_Recaudado_Octubre = "RECAUDADO_OCTUBRE";
        public const String Campo_Recaudado_Noviembre = "RECAUDADO_NOVIEMBRE";
        public const String Campo_Recaudado_Diciembre = "RECAUDADO_DICIEMBRE";
        public const String Campo_Actualizado = "ACTUALIZADO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Movimiento_Ing_Det
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Movimiento_Ing_Det
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 16/Abril/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Movimiento_Ing_Det
    {
        public const String Tabla_Ope_Psp_Movimiento_Ing_Det = "OPE_PSP_MOVIMIENTO_ING_DET";
        public const String Campo_Movimiento_Ing_ID = "MOVIMIENTO_ING_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Rubro_ID = "RUBRO_ID";
        public const String Campo_Tipo_ID = "TIPO_ID";
        public const String Campo_Clase_Ing_ID = "CLASE_ING_ID";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
        public const String Campo_SubConcepto_Ing_ID = "SUBCONCEPTO_ING_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_No_Movimiento_Ing = "NO_MOVIMIENTO_ING";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Importe_Aprobado = "IMPORTE_APROBADO";
        public const String Campo_Importe_Ampliacion = "IMPORTE_AMPLIACION";
        public const String Campo_Importe_Reduccion = "IMPORTE_REDUCCION";
        public const String Campo_Importe_Modificado = "IMPORTE_MODIFICADO";
        public const String Campo_Tipo_Movimiento = "TIPO_MOVIMIENTO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Comentario = "COMENTARIO";
        public const String Campo_Tipo_Concepto = "TIPO_CONCEPTO";
        public const String Campo_Nombre_Documento = "NOMBRE_DOCUMENTO";
        public const String Campo_Ruta_Documento = "RUTA_DOCUMENTO";
        public const String Campo_Extension_Documento = "EXTENSION_DOCUMENTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Poliza_Presupuestal = "NO_POLIZA_PRESUPUESTAL ";
        public const String Campo_Tipo_Poliza_ID_Presupuestal = "TIPO_POLIZA_ID_PRESUPUESTAL";
        public const String Campo_Mes_Anio_Presupuestal = "MES_ANIO_PRESUPUESTAL";
    }


    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Registro_Mov_Ingreso
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Registro_Mov_Ingreso
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 28/Agosto/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Registro_Mov_Ingreso
    {
        public const String Tabla_Ope_Psp_Registro_Mov_Ingreso = "OPE_PSP_REGISTRO_MOV_INGRESO";
        public const String Campo_No_Movimiento = "NO_MOVIMIENTO";
        public const String Campo_Cargo = "CARGO";
        public const String Campo_Abono = "ABONO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Referencia = "REFERENCIA";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Rubro_ID = "RUBRO_ID";
        public const String Campo_Tipo_ID = "TIPO_ID";
        public const String Campo_Clase_Ing_ID = "CLASE_ING_ID";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
        public const String Campo_SubConcepto_Ing_ID = "SUBCONCEPTO_ING_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Poliza_Presupuestal = "NO_POLIZA_PRESUPUESTAL ";
        public const String Campo_Tipo_Poliza_ID_Presupuestal = "TIPO_POLIZA_ID_PRESUPUESTAL";
        public const String Campo_Mes_Anio_Presupuestal = "MES_ANIO_PRESUPUESTAL";
    }


    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Presupuesto_Egr_Esp
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Presupuesto_Egr_Esp
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 12/MAYO/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Presupuesto_Egr_Esp
    {
        public const String Tabla_Ope_Psp_Presupuesto_Egr_Esp = "OPE_PSP_PRESUPUESTO_EGR_ESP";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Tipo_Egreso = "TIPO_EGRESO";
        public const String Campo_No_Modificacion = "NO_MOVIMIENTO";
        public const String Campo_Aprobado = "APROBADO";
        public const String Campo_Ampliacion = "AMPLIACION";
        public const String Campo_Reduccion = "REDUCCION";
        public const String Campo_Modificado = "MODIFICADO";
        public const String Campo_Devengado = "DEVENGADO";
        public const String Campo_Ejercido = "EJERCIDO";
        public const String Campo_Pagado = "PAGADO";
        public const String Campo_Pre_Comprometido = "PRE_COMPROMETIDO";
        public const String Campo_Comprometido = "COMPROMETIDO";
        public const String Campo_Disponible = "DISPONIBLE";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Actualizado = "ACTUALIZADO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Pre_Comprometido_Enero = "PRE_COMPROMETIDO_ENERO";
        public const String Campo_Pre_Comprometido_Febrero = "PRE_COMPROMETIDO_FEBRERO";
        public const String Campo_Pre_Comprometido_Marzo = "PRE_COMPROMETIDO_MARZO";
        public const String Campo_Pre_Comprometido_Abril = "PRE_COMPROMETIDO_ABRIL";
        public const String Campo_Pre_Comprometido_Mayo = "PRE_COMPROMETIDO_MAYO";
        public const String Campo_Pre_Comprometido_Junio = "PRE_COMPROMETIDO_JUNIO";
        public const String Campo_Pre_Comprometido_Julio = "PRE_COMPROMETIDO_JULIO";
        public const String Campo_Pre_Comprometido_Agosto = "PRE_COMPROMETIDO_AGOSTO";
        public const String Campo_Pre_Comprometido_Septiembre = "PRE_COMPROMETIDO_SEPTIEMBRE";
        public const String Campo_Pre_Comprometido_Octubre = "PRE_COMPROMETIDO_OCTUBRE";
        public const String Campo_Pre_Comprometido_Noviembre = "PRE_COMPROMETIDO_NOVIEMBRE";
        public const String Campo_Pre_Comprometido_Diciembre = "PRE_COMPROMETIDO_DICIEMBRE";
        public const String Campo_Comprometido_Enero = "COMPROMETIDO_ENERO";
        public const String Campo_Comprometido_Febrero = "COMPROMETIDO_FEBRERO";
        public const String Campo_Comprometido_Marzo = "COMPROMETIDO_MARZO";
        public const String Campo_Comprometido_Abril = "COMPROMETIDO_ABRIL";
        public const String Campo_Comprometido_Mayo = "COMPROMETIDO_MAYO";
        public const String Campo_Comprometido_Junio = "COMPROMETIDO_JUNIO";
        public const String Campo_Comprometido_Julio = "COMPROMETIDO_JULIO";
        public const String Campo_Comprometido_Agosto = "COMPROMETIDO_AGOSTO";
        public const String Campo_Comprometido_Septiembre = "COMPROMETIDO_SEPTIEMBRE";
        public const String Campo_Comprometido_Octubre = "COMPROMETIDO_OCTUBRE";
        public const String Campo_Comprometido_Noviembre = "COMPROMETIDO_NOVIEMBRE";
        public const String Campo_Comprometido_Diciembre = "COMPROMETIDO_DICIEMBRE";
        public const String Campo_Devengado_Enero = "DEVENGADO_ENERO";
        public const String Campo_Devengado_Febrero = "DEVENGADO_FEBRERO";
        public const String Campo_Devengado_Marzo = "DEVENGADO_MARZO";
        public const String Campo_Devengado_Abril = "DEVENGADO_ABRIL";
        public const String Campo_Devengado_Mayo = "DEVENGADO_MAYO";
        public const String Campo_Devengado_Junio = "DEVENGADO_JUNIO";
        public const String Campo_Devengado_Julio = "DEVENGADO_JULIO";
        public const String Campo_Devengado_Agosto = "DEVENGADO_AGOSTO";
        public const String Campo_Devengado_Septiembre = "DEVENGADO_SEPTIEMBRE";
        public const String Campo_Devengado_Octubre = "DEVENGADO_OCTUBRE";
        public const String Campo_Devengado_Noviembre = "DEVENGADO_NOVIEMBRE";
        public const String Campo_Devengado_Diciembre = "DEVENGADO_DICIEMBRE";
        public const String Campo_Ejercido_Enero = "EJERCIDO_ENERO";
        public const String Campo_Ejercido_Febrero = "EJERCIDO_FEBRERO";
        public const String Campo_Ejercido_Marzo = "EJERCIDO_MARZO";
        public const String Campo_Ejercido_Abril = "EJERCIDO_ABRIL";
        public const String Campo_Ejercido_Mayo = "EJERCIDO_MAYO";
        public const String Campo_Ejercido_Junio = "EJERCIDO_JUNIO";
        public const String Campo_Ejercido_Julio = "EJERCIDO_JULIO";
        public const String Campo_Ejercido_Agosto = "EJERCIDO_AGOSTO";
        public const String Campo_Ejercido_Septiembre = "EJERCIDO_SEPTIEMBRE";
        public const String Campo_Ejercido_Octubre = "EJERCIDO_OCTUBRE";
        public const String Campo_Ejercido_Noviembre = "EJERCIDO_NOVIEMBRE";
        public const String Campo_Ejercido_Diciembre = "EJERCIDO_DICIEMBRE";
        public const String Campo_Pagado_Enero = "PAGADO_ENERO";
        public const String Campo_Pagado_Febrero = "PAGADO_FEBRERO";
        public const String Campo_Pagado_Marzo = "PAGADO_MARZO";
        public const String Campo_Pagado_Abril = "PAGADO_ABRIL";
        public const String Campo_Pagado_Mayo = "PAGADO_MAYO";
        public const String Campo_Pagado_Junio = "PAGADO_JUNIO";
        public const String Campo_Pagado_Julio = "PAGADO_JULIO";
        public const String Campo_Pagado_Agosto = "PAGADO_AGOSTO";
        public const String Campo_Pagado_Septiembre = "PAGADO_SEPTIEMBRE";
        public const String Campo_Pagado_Octubre = "PAGADO_OCTUBRE";
        public const String Campo_Pagado_Noviembre = "PAGADO_NOVIEMBRE";
        public const String Campo_Pagado_Diciembre = "PAGADO_DICIEMBRE";
        public const String Campo_Disponible_Enero = "DISPONIBLE_ENERO";
        public const String Campo_Disponible_Febrero = "DISPONIBLE_FEBRERO";
        public const String Campo_Disponible_Marzo = "DISPONIBLE_MARZO";
        public const String Campo_Disponible_Abril = "DISPONIBLE_ABRIL";
        public const String Campo_Disponible_Mayo = "DISPONIBLE_MAYO";
        public const String Campo_Disponible_Junio = "DISPONIBLE_JUNIO";
        public const String Campo_Disponible_Julio = "DISPONIBLE_JULIO";
        public const String Campo_Disponible_Agosto = "DISPONIBLE_AGOSTO";
        public const String Campo_Disponible_Septiembre = "DISPONIBLE_SEPTIEMBRE";
        public const String Campo_Disponible_Octubre = "DISPONIBLE_OCTUBRE";
        public const String Campo_Disponible_Noviembre = "DISPONIBLE_NOVIEMBRE";
        public const String Campo_Disponible_Diciembre = "DISPONIBLE_DICIEMBRE";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Psp_Concepto_Ing
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Cat_Psp_Concepto_Ing
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 20/Marzo/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Psp_Concepto_Ing
    {
        public const String Tabla_Cat_Psp_Concepto_Ing = "CAT_PSP_CONCEPTO_ING";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
        public const String Campo_Clase_Ing_ID = "CLASE_ING_ID";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Clave = "CLAVE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Banco_ID = "BANCO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Movimiento_Egr_Det
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Movimiento_Egr_Det
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 12/Mayo/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Movimiento_Egr_Det
    {
        public const String Tabla_Ope_Psp_Movimiento_Egr_Det = "OPE_PSP_MOVIMIENTO_EGR_DET";
        public const String Campo_Solicitud_ID = "SOLICITUD_ID";
        public const String Campo_Movimiento_ID = "MOVIMIENTO_ID";
        public const String Campo_Fuente_Financiamiento_ID = "FUENTE_FINANCIAMIENTO_ID";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_No_Movimiento_Egr = "NO_MOVIMIENTO_EGR";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Importe_Aprobado = "IMPORTE_APROBADO";
        public const String Campo_Importe_Ampliacion = "IMPORTE_AMPLIACION";
        public const String Campo_Importe_Reduccion = "IMPORTE_REDUCCION";
        public const String Campo_Importe_Modificado = "IMPORTE_MODIFICADO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Tipo_Operacion = "TIPO_OPERACION";
        public const String Campo_Tipo_Partida = "TIPO_PARTIDA";
        public const String Campo_Tipo_Egreso = "TIPO_EGRESO";
        public const String Campo_Tipo_Movimiento = "TIPO_MOVIMIENTO";
        public const String Campo_Tipo_Usuario = "TIPO_USUARIO";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Comentario = "COMENTARIO";
        public const String Campo_Nombre_Solicitante = "NOMBRE_SOLICITANTE";
        public const String Campo_Puesto_Solicitante = "PUESTO_SOLICITANTE";
        public const String Campo_Nombre_Director = "NOMBRE_DIRECTOR";
        public const String Campo_Puesto_Director = "PUESTO_DIRECTOR";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Poliza_Presupuestal = "NO_POLIZA_PRESUPUESTAL ";
        public const String Campo_Tipo_Poliza_ID_Presupuestal = "TIPO_POLIZA_ID_PRESUPUESTAL";
        public const String Campo_Mes_Anio_Presupuestal = "MES_ANIO_PRESUPUESTAL";

    }
    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Cat_Sap_Det_Fte_Concepto
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Cat_Sap_Det_Fte_Concepto
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 22/Junio/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Cat_Sap_Det_Fte_Concepto
    {
        public const String Tabla_Cat_Sap_Det_Fte_Concepto = "CAT_SAP_DET_FTE_CONCEPTO";
        public const String Campo_Fuente_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Concepto_Ing_ID = "CONCEPTO_ING_ID";
    }
    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Movimiento_Egr
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Movimiento_Egr
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 12/MAYO/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Movimiento_Egr
    {
        public const String Tabla_Ope_Psp_Movimiento_Egr = "OPE_PSP_MOVIMIENTO_EGR";
        public const String Campo_No_Movimiento_Egr = "NO_MOVIMIENTO_EGR";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Tipo_Egreso = "TIPO_EGRESO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Total_Modificado = "TOTAL_MODIFICADO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : OPE_PSP_HIST_CALENDAR_PRESU
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla OPE_PSP_HIST_CALENDAR_PRESU
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 23/Noviembre/2011
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Hist_Calendar_Presu
    {
        public const String Tabla_Ope_Psp_Hist_Calendar_Presu = "OPE_PSP_HIST_CALENDAR_PRESU";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Comentario = "COMENTARIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Proyecto_Programa_Id = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }
    ///****************************************************************************************************************************************************************
    ///NOMBRE: Ope_Psp_Comentarios_Mov
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_PSP_COMENTARIO_MOV
    ///
    ///CREO: Hugo Enrique Ramírez Aguilera
    ///FECHA CREÓ: 17/Noviembre/2011
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Ope_Psp_Comentarios_Mov
    {
        public const String Tabla_Ope_Psp_Comentarios_Mov = "OPE_PSP_COMENTARIOS_MOV";
        public const String Campo_Numero_Solicitud = "NO_SOLICITUD";
        public const String Campo_Comentario = "COMENTARIO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///****************************************************************************************************************************************************************
    ///NOMBRE: Ope_Psp_Cierre_Presup
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_PSP_CIERRE_PRESUP
    ///
    ///CREO: Hugo Enrique Ramírez Aguilera
    ///FECHA CREÓ: 17/Noviembre/2011
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Ope_Psp_Cierre_Presup
    {
        public const String Tabla_Ope_Psp_Cierre_Presup = "OPE_PSP_CIERRE_PRESUP";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Enero = "ENERO";
        public const String Campo_Febrero = "FEBRERO";
        public const String Campo_Marzo = "MARZO";
        public const String Campo_Abril = "ABRIL";
        public const String Campo_Mayo = "MAYO";
        public const String Campo_Junio = "JUNIO";
        public const String Campo_Julio = "JULIO";
        public const String Campo_Agosto = "AGOSTO";
        public const String Campo_Septiembre = "SEPTIEMBRE";
        public const String Campo_Octubre = "OCTUBRE";
        public const String Campo_Noviembre = "NOVIEMBRE";
        public const String Campo_Diciembre = "DICIEMBRE";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : OPE_PSP_CALENDARIZACION_PRESU
    ///DESCRIPCIÓN          : Clase con contiene los datos de la tabla OPE_COM_SOLICITUD_TRANSF
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 14/Noviembre/2011
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Calendarizacion_Presu
    {
        public const String Tabla_Ope_Psp_Calendarizacion_Presu = "OPE_PSP_CALENDARIZACION_PRESU";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Empleado_ID_Creo = "EMPLEADO_ID_CREO";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Cantidad_Enero = "CANTIDAD_ENERO";
        public const String Campo_Cantidad_Febrero = "CANTIDAD_FEBRERO";
        public const String Campo_Cantidad_Marzo = "CANTIDAD_MARZO";
        public const String Campo_Cantidad_Abril = "CANTIDAD_ABRIL";
        public const String Campo_Cantidad_Mayo = "CANTIDAD_MAYO";
        public const String Campo_Cantidad_Junio = "CANTIDAD_JUNIO";
        public const String Campo_Cantidad_Julio = "CANTIDAD_JULIO";
        public const String Campo_Cantidad_Agosto = "CANTIDAD_AGOSTO";
        public const String Campo_Cantidad_Septiembre = "CANTIDAD_SEPTIEMBRE";
        public const String Campo_Cantidad_Octubre = "CANTIDAD_OCTUBRE";
        public const String Campo_Cantidad_Noviembre = "CANTIDAD_NOVIEMBRE";
        public const String Campo_Cantidad_Diciembre = "CANTIDAD_DICIEMBRE";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Subnivel_Presupuestal_ID = "SUBNIVEL_PRESUPUESTAL_ID";
        public const String Campo_No_Poliza_Presupuestal = "NO_POLIZA_PRESUPUESTAL ";
        public const String Campo_Tipo_Poliza_ID_Presupuestal = "TIPO_POLIZA_ID_PRESUPUESTAL";
        public const String Campo_Mes_Anio_Presupuestal = "MES_ANIO_PRESUPUESTAL";
        public const String Campo_Validacion = "Validacion";
        public const String Campo_Comentarios = "Comentarios";

    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE: Ope_Cat_Documentos_Avaluo_Av
    /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_CAT_DOC_AVALUO_UR_AV
    /// PARÁMETROS :     
    /// CREO       : Miguel Angel Bedolla Moreno
    /// FECHA_CREO : 17/Mayo/2012 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Psp_Documentos_Egresos
    {
        public const String Tabla_Ope_Psp_Documentos_Egresos = "OPE_PSP_DOCUMENTOS_EGRESOS";
        public const String Campo_Dependencia_Id = "DEPENDENCIA_ID";
        public const String Campo_Partida_Id = "PARTIDA_ID";
        public const String Campo_Producto_Id = "PRODUCTO_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Ruta_Documento = "RUTA_DOCUMENTO";
        public const String Campo_Proyecto_Programa_Id = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Documento = "DOCUMENTO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }



    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Cat_Parametros_Ejercer_Psp
    /// DESCRIPCION:            Clase que contiene los campos de la tabla CAT_PARAMETROS_EJERCER_PSP
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            16/NOV/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Cat_Parametros_Ejercer_Psp
    {
        public const String Tabla_Cat_Parametros_Ejercer_Psp = "CAT_PARAMETROS_EJERCER_PSP";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Tipo_De_Consulta = "TIPO_DE_CONSULTA";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Psp_Reservas
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_PSP_RESERVAS
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            15/NOV/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Psp_Reservas
    {
        public const String Campo_No_Deuda = "NO_DEUDA";
        public const String Tabla_Ope_Psp_Reservas = "OPE_PSP_RESERVAS";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_Concepto = "CONCEPTO";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fte_Financimiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Importe_Inicial = "IMPORTE_INICIAL ";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Beneficiario = "BENEFICIARIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Tipo_Solicitud_Pago_ID = "TIPO_SOLICITUD_PAGO_ID";
        public const String Campo_Tipo_Reserva = "TIPO_RESERVA";
        public const String Campo_Recurso = "RECURSO";
        public const String Campo_Reserva_Directa = "RESERVA_DIRECTA";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Psp_Registro_Movimientos
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_PSP_REGISTRO_MOVIMIETOS
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :            15/NOV/2011 
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Psp_Registro_Movimientos
    {
        public const String Tabla_Ope_Psp_Registro_Movimientos = "OPE_PSP_REGISTRO_MOVIMIENTOS";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_Cargo = "CARGO";
        public const String Campo_Abono = "ABONO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Fecha = "FECHA";
        public const String Campo_Usuario = "USUARIO";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_No_Poliza = "NO_POLIZA";
        public const String Campo_Tipo_Poliza_ID = "TIPO_POLIZA_ID";
        public const String Campo_Mes_Ano = "MES_ANO";
        public const String Campo_Partida = "PARTIDA";
        public const String Campo_No_Poliza_Presupuestal = "NO_POLIZA_PRESUPUESTAL ";
        public const String Campo_Tipo_Poliza_ID_Presupuestal = "TIPO_POLIZA_ID_PRESUPUESTAL";
        public const String Campo_Mes_Anio_Presupuestal = "MES_ANIO_PRESUPUESTAL";
        public const String Campo_Estatus_Movimiento = "ESTATUS_MOVIMIENTO";

    }

    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: OPE_PSP_PRESUPUESTO_APROBADO
    ///DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PSP_PRESUPUESTO_APROBADO
    ///PARAMETROS: 
    ///CREO: Gustavo Angeles Cruz
    ///FECHA_CREO: 15/Noviembre/2011
    ///MODIFICO: 
    ///FECHA_MODIFICO:  
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Ope_Psp_Presupuesto_Aprobado
    {
        public const String Tabla_Ope_Psp_Presupuesto_Aprobado = "OPE_PSP_PRESUPUESTO_APROBADO";
        public const String Campo_No_Presupuesto = "NO_PRESUPUESTO";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Aprobado = "APROBADO";
        public const String Campo_Ampliacion = "AMPLIACION";
        public const String Campo_Reduccion = "REDUCCION";
        public const String Campo_Modificado = "MODIFICADO";
        public const String Campo_Devengado = "DEVENGADO";
        public const String Campo_Ejercido = "EJERCIDO";
        public const String Campo_Pagado = "PAGADO";
        public const String Campo_Pre_Comprometido = "PRE_COMPROMETIDO";
        public const String Campo_Comprometido = "COMPROMETIDO";
        public const String Campo_Disponible = "DISPONIBLE";
        public const String Campo_Saldo = "SALDO";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Importe_Enero = "IMPORTE_ENERO";
        public const String Campo_Importe_Febrero = "IMPORTE_FEBRERO";
        public const String Campo_Importe_Marzo = "IMPORTE_MARZO";
        public const String Campo_Importe_Abril = "IMPORTE_ABRIL";
        public const String Campo_Importe_Mayo = "IMPORTE_MAYO";
        public const String Campo_Importe_Junio = "IMPORTE_JUNIO";
        public const String Campo_Importe_Julio = "IMPORTE_JULIO";
        public const String Campo_Importe_Agosto = "IMPORTE_AGOSTO";
        public const String Campo_Importe_Septiembre = "IMPORTE_SEPTIEMBRE";
        public const String Campo_Importe_Octubre = "IMPORTE_OCTUBRE";
        public const String Campo_Importe_Noviembre = "IMPORTE_NOVIEMBRE";
        public const String Campo_Importe_Diciembre = "IMPORTE_DICIEMBRE";
        public const String Campo_Importe_Total = "IMPORTE_TOTAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Afectado_Enero = "AFECTADO_ENERO";
        public const String Campo_Afectado_Febrero = "AFECTADO_FEBRERO";
        public const String Campo_Afectado_Marzo = "AFECTADO_MARZO";
        public const String Campo_Afectado_Abril = "AFECTADO_ABRIL";
        public const String Campo_Afectado_Mayo = "AFECTADO_MAYO";
        public const String Campo_Afectado_Junio = "AFECTADO_JUNIO";
        public const String Campo_Afectado_Julio = "AFECTADO_JULIO";
        public const String Campo_Afectado_Agosto = "AFECTADO_AGOSTO";
        public const String Campo_Afectado_Septiembre = "AFECTADO_SEPTIEMBRE";
        public const String Campo_Afectado_Octubre = "AFECTADO_OCTUBRE";
        public const String Campo_Afectado_Noviembre = "AFECTADO_NOVIEMBRE";
        public const String Campo_Afectado_Diciembre = "AFECTADO_DICIEMBRE";
        public const String Campo_Subnivel_Presupuestal_ID = "SUBNIVEL_PRESUPUESTAL_ID";
        public const String Campo_Nivel_Presupuesto = "NIVEL_PRESUPUESTO";
        public const String Campo_Disponible_Enero = "DISPONIBLE_ENERO";
        public const String Campo_Disponible_Febrero = "DISPONIBLE_FEBRERO";
        public const String Campo_Disponible_Marzo = "DISPONIBLE_MARZO";
        public const String Campo_Disponible_Abril = "DISPONIBLE_ABRIL";
        public const String Campo_Disponible_Mayo = "DISPONIBLE_MAYO";
        public const String Campo_Disponible_Junio = "DISPONIBLE_JUNIO";
        public const String Campo_Disponible_Julio = "DISPONIBLE_JULIO";
        public const String Campo_Disponible_Agosto = "DISPONIBLE_AGOSTO";
        public const String Campo_Disponible_Septiembre = "DISPONIBLE_SEPTIEMBRE";
        public const String Campo_Disponible_Octubre = "DISPONIBLE_OCTUBRE";
        public const String Campo_Disponible_Noviembre = "DISPONIBLE_NOVIEMBRE";
        public const String Campo_Disponible_Diciembre = "DISPONIBLE_DICIEMBRE";
        public const String Campo_Pre_Comprometido_Enero = "PRE_COMPROMETIDO_ENERO";
        public const String Campo_Pre_Comprometido_Febrero = "PRE_COMPROMETIDO_FEBRERO";
        public const String Campo_Pre_Comprometido_Marzo = "PRE_COMPROMETIDO_MARZO";
        public const String Campo_Pre_Comprometido_Abril = "PRE_COMPROMETIDO_ABRIL";
        public const String Campo_Pre_Comprometido_Mayo = "PRE_COMPROMETIDO_MAYO";
        public const String Campo_Pre_Comprometido_Junio = "PRE_COMPROMETIDO_JUNIO";
        public const String Campo_Pre_Comprometido_Julio = "PRE_COMPROMETIDO_JULIO";
        public const String Campo_Pre_Comprometido_Agosto = "PRE_COMPROMETIDO_AGOSTO";
        public const String Campo_Pre_Comprometido_Septiembre = "PRE_COMPROMETIDO_SEPTIEMBRE";
        public const String Campo_Pre_Comprometido_Octubre = "PRE_COMPROMETIDO_OCTUBRE";
        public const String Campo_Pre_Comprometido_Noviembre = "PRE_COMPROMETIDO_NOVIEMBRE";
        public const String Campo_Pre_Comprometido_Diciembre = "PRE_COMPROMETIDO_DICIEMBRE";
        public const String Campo_Comprometido_Enero = "COMPROMETIDO_ENERO";
        public const String Campo_Comprometido_Febrero = "COMPROMETIDO_FEBRERO";
        public const String Campo_Comprometido_Marzo = "COMPROMETIDO_MARZO";
        public const String Campo_Comprometido_Abril = "COMPROMETIDO_ABRIL";
        public const String Campo_Comprometido_Mayo = "COMPROMETIDO_MAYO";
        public const String Campo_Comprometido_Junio = "COMPROMETIDO_JUNIO";
        public const String Campo_Comprometido_Julio = "COMPROMETIDO_JULIO";
        public const String Campo_Comprometido_Agosto = "COMPROMETIDO_AGOSTO";
        public const String Campo_Comprometido_Septiembre = "COMPROMETIDO_SEPTIEMBRE";
        public const String Campo_Comprometido_Octubre = "COMPROMETIDO_OCTUBRE";
        public const String Campo_Comprometido_Noviembre = "COMPROMETIDO_NOVIEMBRE";
        public const String Campo_Comprometido_Diciembre = "COMPROMETIDO_DICIEMBRE";
        public const String Campo_Devengado_Enero = "DEVENGADO_ENERO";
        public const String Campo_Devengado_Febrero = "DEVENGADO_FEBRERO";
        public const String Campo_Devengado_Marzo = "DEVENGADO_MARZO";
        public const String Campo_Devengado_Abril = "DEVENGADO_ABRIL";
        public const String Campo_Devengado_Mayo = "DEVENGADO_MAYO";
        public const String Campo_Devengado_Junio = "DEVENGADO_JUNIO";
        public const String Campo_Devengado_Julio = "DEVENGADO_JULIO";
        public const String Campo_Devengado_Agosto = "DEVENGADO_AGOSTO";
        public const String Campo_Devengado_Septiembre = "DEVENGADO_SEPTIEMBRE";
        public const String Campo_Devengado_Octubre = "DEVENGADO_OCTUBRE";
        public const String Campo_Devengado_Noviembre = "DEVENGADO_NOVIEMBRE";
        public const String Campo_Devengado_Diciembre = "DEVENGADO_DICIEMBRE";
        public const String Campo_Ejercido_Enero = "EJERCIDO_ENERO";
        public const String Campo_Ejercido_Febrero = "EJERCIDO_FEBRERO";
        public const String Campo_Ejercido_Marzo = "EJERCIDO_MARZO";
        public const String Campo_Ejercido_Abril = "EJERCIDO_ABRIL";
        public const String Campo_Ejercido_Mayo = "EJERCIDO_MAYO";
        public const String Campo_Ejercido_Junio = "EJERCIDO_JUNIO";
        public const String Campo_Ejercido_Julio = "EJERCIDO_JULIO";
        public const String Campo_Ejercido_Agosto = "EJERCIDO_AGOSTO";
        public const String Campo_Ejercido_Septiembre = "EJERCIDO_SEPTIEMBRE";
        public const String Campo_Ejercido_Octubre = "EJERCIDO_OCTUBRE";
        public const String Campo_Ejercido_Noviembre = "EJERCIDO_NOVIEMBRE";
        public const String Campo_Ejercido_Diciembre = "EJERCIDO_DICIEMBRE";
        public const String Campo_Pagado_Enero = "PAGADO_ENERO";
        public const String Campo_Pagado_Febrero = "PAGADO_FEBRERO";
        public const String Campo_Pagado_Marzo = "PAGADO_MARZO";
        public const String Campo_Pagado_Abril = "PAGADO_ABRIL";
        public const String Campo_Pagado_Mayo = "PAGADO_MAYO";
        public const String Campo_Pagado_Junio = "PAGADO_JUNIO";
        public const String Campo_Pagado_Julio = "PAGADO_JULIO";
        public const String Campo_Pagado_Agosto = "PAGADO_AGOSTO";
        public const String Campo_Pagado_Septiembre = "PAGADO_SEPTIEMBRE";
        public const String Campo_Pagado_Octubre = "PAGADO_OCTUBRE";
        public const String Campo_Pagado_Noviembre = "PAGADO_NOVIEMBRE";
        public const String Campo_Pagado_Diciembre = "PAGADO_DICIEMBRE";
        public const String Campo_Actualizado = "ACTUALIZADO";
        public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
        public const String Campo_Tipo = "TIPO";
    }
    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Cat_Psp_Parametros
    ///DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PSP_PARAMETROS
    ///PARAMETROS: 
    ///CREO: Francisco Antonio Gallardo Castañeda
    ///FECHA_CREO: 18/Octubre/2011
    ///MODIFICO: 
    ///FECHA_MODIFICO:  
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Psp_Parametros
    {
        public const String Tabla_Cat_Psp_Parametros = "CAT_PSP_PARAMETROS";
        public const String Campo_Parametro_ID = "PARAMETRO_ID";
        public const String Campo_Fecha_Apertura = "FECHA_APERTURA";
        public const String Campo_Fecha_Cierre = "FECHA_CIERRE";
        public const String Campo_Anio_Presupuestar = "ANIO_PRESUPUESTAR";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///******************************************************************************* 
    ///NOMBRE DE LA CLASE: Cat_Psp_Parametros_Detalles
    ///DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PSP_PARAMETROS_DETALLES
    ///PARAMETROS: 
    ///CREO: Francisco Antonio Gallardo Castañeda
    ///FECHA_CREO: 18/Octubre/2011
    ///MODIFICO: 
    ///FECHA_MODIFICO:  
    ///CAUSA_MODIFICACIÓN:
    ///*******************************************************************************
    public class Cat_Psp_Parametros_Detalles
    {
        public const String Tabla_Cat_Psp_Parametros_Detalles = "CAT_PSP_PARAMETROS_DETALLES";
        public const String Campo_Parametro_ID = "PARAMETRO_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Psp_Limmite_presupuestal
    /// DESCRIPCION: 
    /// PARAMETROS :
    /// CREO       : Gustavo Angeles Cruz
    /// FECHA_CREO : 19 / oct /2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Psp_Limite_presupuestal
    {
        public const String Tabla_Ope_Psp_Limite_presupuestal = "OPE_PSP_LIMITE_PRESUPUESTAL";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Limite_Presupuestal = "LIMITE_PRESUPUESTAL";
        public const String Campo_Anio_presupuestal = "ANIO_PRESUPUESTAL";
        public const String Campo_Fte_Financiamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///*******************************************************************************
    /// NOMBRE DE LA FUNCION: Ope_Psp_Detalle_Lim_Presup
    /// DESCRIPCION: 
    /// PARAMETROS :
    /// CREO       : Gustavo Angeles Cruz
    /// FECHA_CREO : 19 / oct /2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Psp_Detalle_Lim_Presup
    {
        public const String Tabla_Ope_Psp_Limite_presupuestal = "OPE_PSP_DETALLE_LIM_PRESUP";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_Empleado_ID = "EMPLEADO_ID";
        public const String Campo_Capitulo_ID = "CAPITULO_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Encabezado = "Encabezado";
        public const String Campo_Proyecto_Programa_Id = "Proyecto_Programa_Id";
        public const String Campo_Anio_presupuestal = "ANIO_PRESUPUESTAL";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }

    ///****************************************************************************************************************************************************************
    ///NOMBRE: Cat_Ope_Com_Solicitud_Transf
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla OPE_COM_SOLICITUD_TRANSF
    ///
    ///CREO: Hugo Enrique Ramírez Aguilera
    ///FECHA CREÓ: 18/Octubre/2011
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Cat_Ope_Com_Solicitud_Transf
    {
        public const String Tabla_Cat_Ope_Com_Solicitud_Transf = "OPE_COM_SOLICITUD_TRANSF";
        public const String Campo_No_Solicitud = "NO_SOLICITUD";
        public const String Campo_Codigo1 = "CODIGO1";
        public const String Campo_Codigo2 = "CODIGO2";
        public const String Campo_Importe = "IMPORTE";
        public const String Campo_Justificacion = "JUSTIFICACION";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Justificacion_Solicitud = "JUSTIFICACION_SOLICITUD";
        public const String Campo_Tipo_Operacion = "TIPO_OPERACION";
        public const String Campo_Origen_Fuente_Financiamiento_Id = "ORIGEN_FTE_FINANCIAMIENTO_ID";
        public const String Campo_Destino_Fuente_Financiamiento_Id = "DESTINO_FTE_FINANCIAMIENTO_ID";
        public const String Campo_Origen_Area_Funcional_Id = "ORIGEN_AREA_FUNCIONAL_ID";
        public const String Campo_Destino_Area_Funcional_Id = "DESTINO_AREA_FUNCIONAL_ID";
        public const String Campo_Origen_Programa_Id = "ORIGEN_PROGRAMA_ID";
        public const String Campo_Destino_Programa_Id = "DESTINO_PROGRAMA_ID";
        public const String Campo_Origen_Partida_Id = "ORIGEN_PARTIDA_ID";
        public const String Campo_Destino_Partida_Id = "DESTINO_PARTIDA_ID";
        public const String Campo_Origen_Dependencia_Id = "ORIGEN_DEPENDENCIA_ID";
        public const String Campo_Destino_Dependencia_Id = "DESTINO_DEPENDENCIA_ID";
        public const String Campo_Mes_Origen = "MES_ORIGEN";
        public const String Campo_Mes_Destino = "MES_DESTINO";
        public const String Campo_No_Reserva = "NO_RESERVA";
        public const String Campo_Anio = "ANIO";
    }

    ///****************************************************************************************************************************************************************
    ///NOMBRE: Cat_PSP_Subnivel_Presupuestos
    ///
    ///DESCRIPCIÓN: Clase con contiene los datos de la tabla CAT_PSP_SUBNIVEL_PRESUPUESTOS
    ///
    ///CREO: Jennyfer Ivonne Ceja Lemus
    ///FECHA CREÓ: 17/Agosto/2012
    ///MODIFICO:
    ///FECHA MODIFICO:
    ///CAUSA MODIFICACION:
    ///****************************************************************************************************************************************************************
    public class Cat_Psp_SubNivel_Presupuestos
    {
        public const String Tabla_Cat_PSP_Subnivel_Presupuestos = "CAT_PSP_SUBNIVEL_PRESUPUESTOS";
        public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
        public const String Campo_FTE_Fincanciamiento_ID = "FTE_FINANCIAMIENTO_ID";
        public const String Campo_Proyecto_Programa_ID = "PROYECTO_PROGRAMA_ID";
        public const String Campo_Partida_ID = "PARTIDA_ID";
        public const String Campo_Area_Funcional_ID = "AREA_FUNCIONAL_ID";
        public const String Campo_Subnivel_Presupuestal = "SUBNIVEL_PRESUPUESTAL";
        public const String Campo_Anio = "ANIO";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Subnivel_Presupuestal_ID = "SUBNIVEL_PRESUPUESTAL_ID";
        public const String Campo_Codigo_Programatico = "CODIGO_PROGRAMATICO";
    }

    ///*******************************************************************************
    ///NOMBRE DE LA FUNCIÓN : Ope_Psp_Movimiento_Egr_Det_Doc
    ///DESCRIPCIÓN          : Clase contiene los datos de la tabla Ope_Psp_Movimiento_Egr_Det_Doc
    ///PARAMETROS           :
    ///CREO                 : Leslie Gonzalez Vázquez
    ///FECHA_CREO           : 05/Septiembre/2012
    ///MODIFICO             :
    ///FECHA_MODIFICO       :
    ///CAUSA_MODIFICACIÓN   :
    ///*******************************************************************************
    public class Ope_Psp_Movimiento_Egr_Det_Doc
    {
        public const String Tabla_Ope_Psp_Movimiento_Egr_Det_Doc = "OPE_PSP_MOVIMIENTO_EGR_DET_DOC";
        public const String Campo_Anexo_ID = "ANEXO_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Ruta_Documento = "RUTA_DOCUMENTO";
        public const String Campo_Extension = "EXTENSION";
        public const String Campo_Solicitud_ID = "SOLICITUD_ID";
        public const String Campo_Movimiento_ID = "MOVIMIENTO_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }



    #endregion

    ///*************************************************************************************************************************
    ///                                                                MULTIALMACENES
    ///*************************************************************************************************************************
    ///

    #region Multialmacenes

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Cat_Alm_Multialmacenes
    /// DESCRIPCION:            Clase que contiene los campos de la tabla CAT_ALM_MULTIALMACENES
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           6/DIC/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    ///
    public class Cat_Alm_Multialmacenes
    {
        public const String Tabla_Cat_Alm_Multialmacenes = "CAT_ALM_MULTIALMACENES";
        public const String Campo_Almacen_ID = "ALMACEN_ID";
        public const String Campo_Nombre = "NOMBRE";
        public const String Campo_Descripcion = "DESCRIPCION";
        public const String Campo_Responsable_ID = "RESPONSABLE_ID";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Alm_Traspasos
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_ALM_TRASPASOS
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           6/DIC/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Alm_Traspasos
    {
        public const String Tabla_Ope_Alm_Traspasos = "OPE_ALM_TRASPASOS";
        public const String Campo_No_Traspaso = "NO_TRASPASO";
        public const String Campo_No_Requisicion = "NO_REQUISICION";
        public const String Campo_Almacen_ID = "ALMACEN_ID";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
    }


    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Alm_Productos_Multi
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_ALM_PRODUCTOS_MULTI
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           6/DIC/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Alm_Productos_Multi
    {
        public const String Tabla_Ope_Alm_Productos_Multi = "OPE_ALM_PRODUCTOS_MULTI";
        public const String Campo_Almacen_ID = "ALMACEN_ID";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Existencia = "EXISTENCIA";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:     Ope_Alm_Salidas_Multi
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_ALM_SALIDAS_MULTI
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           6/DIC/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Alm_Salidas_Multi
    {
        public const String Tabla_Ope_Alm_Salidas_Multi = "OPE_ALM_SALIDAS_MULTI";
        public const String Campo_No_Salida = "NO_SALIDA";
        public const String Campo_Almacen_ID = "ALMACEN_ID";
        public const String Campo_Estatus = "ESTATUS";
        public const String Campo_Empleado_Recibio_ID = "EMPLEADO_RECIBIO_ID";
        public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
        public const String Campo_Fecha_Hora = "FECHA_HORA";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_IVA = "IVA";
        public const String Campo_Subtotal = "SUBTOTAL";
        public const String Campo_Total = "TOTAL";


    }

    ///*******************************************************************************
    /// NOMBRE DE LA CLASE:    Ope_Alm_Salidas_Det_Multi
    /// DESCRIPCION:            Clase que contiene los campos de la tabla OPE_ALM_SALIDAS_DET_MULTI
    /// PARAMETROS :     
    /// CREO       :           Susana Trigueros Armenta
    /// FECHA_CREO :           6/DIC/2011
    /// MODIFICO          :
    /// FECHA_MODIFICO    :
    /// CAUSA_MODIFICACION:
    ///*******************************************************************************
    public class Ope_Alm_Salidas_Det_Multi
    {
        public const String Tabla_Ope_Alm_Salidas_Det_Multi = "OPE_ALM_SALIDAS_DET_MULTI";
        public const String Campo_No_Salida = "NO_SALIDA";
        public const String Campo_Producto_ID = "PRODUCTO_ID";
        public const String Campo_Cantidad = "CANTIDAD";
        public const String Campo_Usuario_Creo = "USUARIO_CREO";
        public const String Campo_Fecha_Creo = "FECHA_CREO";
        public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        public const String Campo_Precio_Unitario = "PRECIO_UNITARIO";
        public const String Campo_Precio_c_IVA = "PRECIO_C_IVA";
        public const String Campo_IVA = "IVA";
        public const String Campo_Subtotal = "SUBTOTAL";
        public const String Campo_Total = "TOTAL";
    }


    #endregion

    ///*************************************************************************************************************************
    ///                                                                COMERCIALIZACION
    ///*************************************************************************************************************************
    #region Comercialización

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Calles
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_CALLES
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 15/Julio/2011
        /// MODIFICO          : Josué Daniel Sámano García
        /// FECHA_MODIFICO    : 01/Agosto/2011
        /// CAUSA_MODIFICACIÓN: Se modificó la tabla de la base de datos, por lo tanto también se tienen que modificar las constantes
        ///*******************************************************************************
        public class Cat_Cor_Calles
        {
            public const String Tabla_Cat_Cor_Calles = "CAT_COR_CALLES";
            public const String Tabla_Cat_Cor_Vialidades = "CAT_COR_VIALIDADES";
            public const String Campo_Calle_ID = "CALLE_ID";
            public const String Campo_Clave_Calle = "CLAVE_PADRON_USUARIO";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Vialidad_ID = "VIALIDAD_ID";
            public const String Campo_Nombre_Vialidad = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Calles_Colonias
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_CALLES_COLONIAS
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 03/Septiembre/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACIÓN: 
        ///*******************************************************************************
        public class Cat_Cor_Calles_Colonias
        {
            public const String Tabla_Cat_Cor_Calles_Colonias = "CAT_COR_CALLES_COLONIAS";
            public const String Campo_Calle_Colonia_ID = "CALLE_COLONIA_ID";
            public const String Campo_Calle_ID = "CALLE_ID";
            public const String Campo_Colonia_ID = "COLONIA_ID";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Vialidades
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_VIALIDADES
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 01/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACIÓN: 
        ///*******************************************************************************
        public class Cat_Cor_Vialidades
        {
            public const String Tabla_Cat_Cor_Vialidades = "CAT_COR_VIALIDADES";
            public const String Campo_Vialidad_ID = "VIALIDAD_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE_CLASE   : Cat_Cor_Regiones
        /// DESCRIPCION    : Clase que contiene los campos de la tabla CAT_COR_REGIONES
        /// PARAMETROS     :     
        /// CREO           : Sergio Ulises Durán Hernández
        /// FECHA_CREO     : 18-Agosto-2011
        /// MODIFICO       :
        /// FECHA_MODIFICO :
        /// CAUSA_MODIFICO :
        ///*******************************************************************************
        public class Cat_Cor_Regiones
        {
            public const String Tabla_Cat_Cor_Regiones = "CAT_COR_REGIONES";
            public const String Campo_Region_ID = "REGION_ID";
            public const String Campo_Numero_Region = "NUMERO_REGION";
            public const String Campo_Distrito = "DISTRITO";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
    
        ///*******************************************************************************
        /// NOMBRE_CLASE   : Cat_Cor_Estados
        /// DESCRIPCION    : Clase que contiene los campos de la tabla CAT_COR_ESTADOS
        /// PARAMETROS     :     
        /// CREO           : Sergio Ulises Durán Hernández
        /// FECHA_CREO     : 18-Agosto-2011
        /// MODIFICO       :
        /// FECHA_MODIFICO :
        /// CAUSA_MODIFICO :
        ///*******************************************************************************
        public class Cat_Cor_Estados
        {
            public const String Tabla_Cat_Cor_Estados = "CAT_COR_ESTADOS";
            public const String Campo_Estado_ID = "ESTADO_ID";
            public const String Campo_Clave = "CLAVE";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE_CLASE   : Cat_Cor_Ciudades
        /// DESCRIPCION    : Clase que contiene los campos de la tabla CAT_COR_CIUDADES
        /// PARAMETROS     :     
        /// CREO           : Sergio Ulises Durán Hernández
        /// FECHA_CREO     : 18-Agosto-2011
        /// MODIFICO       :
        /// FECHA_MODIFICO :
        /// CAUSA_MODIFICO :
        ///*******************************************************************************
        public class Cat_Cor_Ciudades
        {
            public const String Tabla_Cat_Cor_Ciudades = "CAT_COR_CIUDADES";
            public const String Campo_Ciudad_ID = "CIUDAD_ID";
            public const String Campo_Estado_ID = "ESTADO_ID";
            public const String Campo_Clave = "CLAVE";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Sectores
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_SECTORES
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 20/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Sectores
        {
            public const String Tabla_Cat_Cor_Sectores = "CAT_COR_SECTORES";
            public const String Campo_Sector_ID = "SECTOR_ID";
            public const String Campo_Numero_Sector = "NUMERO_SECTOR";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Giros_Actividades
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla Cat_Cor_Giros_Actividades
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 22/Julio/2011
        /// MODIFICO          : Diego Yañez Rodriguez
        /// FECHA_MODIFICO    : 26 Octubre 2011
        /// CAUSA_MODIFICACIÓN: Se modificaron campos y nombre de la tablas
        ///*******************************************************************************
        public class Cat_Cor_Giros_Actividades
        {
            public const String Tabla_Cat_Cor_Giros_Actividades = "CAT_COR_GIROS_ACTIVIDADES";
            public const String Campo_Actividad_Giro_ID = "Actividad_Giro_ID";
            public const String Campo_Giro_ID = "Giro_ID";
            public const String Campo_Clave = "Clave";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Medidores
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_MEDIDORES
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 22/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Medidores
        {
            public const String Tabla_Cat_Cor_Medidores = "CAT_COR_MEDIDORES";
            public const String Campo_Medidor_ID = "MEDIDOR_ID";
            public const String Campo_Marca_ID = "MARCA_ID";
            public const String Campo_No_Medidor = "NO_MEDIDOR";
            public const String Campo_Diametro = "DIAMETRO";
            public const String Campo_Origen = "ORIGEN";
            public const String Campo_No_Inventario = "NO_INVENTARIO";
            public const String Campo_Constructora_ID = "CONSTRUCTORA_ID";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Medidores_Lecturas_Detalles
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla Cat_Cor_Medidores_Lecturas_Detalles
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 26/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Medidores_Detalles
        {
            public const String Tabla_Cat_Cor_Medidores_Detalles = "Cat_Cor_Medidores_Lecturas_Detalles";
            public const String Campo_Medidor_Detalle_ID = "MEDIDOR_DETALLE_ID";
            public const String Campo_Predio_ID = "PREDIO_ID";
            public const String Campo_Medidor_ID = "MEDIDOR_ID";
            public const String Campo_Lectura_Inicial = "LECTURA_INICIAL";
            public const String Campo_No_Medidor = "NO_MEDIDOR";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_No_Inventario = "NO_INVENTARIO";
            public const String Campo_Fecha_Instalacion = "FECHA_INSTALACION";
            public const String Campo_Fecha_Desinstalacion = "FECHA_DESINSTALACION";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Medidores_Lecturas_Detalles
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_MEDIDORES_LECTURAS_DETALLES
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 26/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Medidores_Lecturas_Detalles
        {
            public const String Tabla_Cat_Cor_Medidores_Lecturas_Detalles = "CAT_COR_MEDIDORES_LECTURAS_DETALLES";
            public const String Campo_Medidor_Lectura_Detalle_ID = "MEDIDOR_LECTURA_DETALLE_ID";
            public const String Campo_Predio_ID = "PREDIO_ID";
            public const String Campo_Medidor_ID = "MEDIDOR_ID";
            public const String Campo_Fecha_Lectura = "FECHA_LECTURA";
            public const String Campo_Lectura = "LECTURA";
            public const String Campo_Motivo_No_Lectura_ID = "MOTIVO_NO_LECTURA_ID";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Predios
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_PREDIOS
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 28/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Predios
        {
            public const String Tabla_Cat_Cor_Predios = "CAT_COR_PREDIOS";
            public const String Campo_Predio_ID = "PREDIO_ID";
            public const String Campo_Zona_ID = "ZONA_ID";
            public const String Campo_Colonia_ID = "COLONIA_ID";
            public const String Campo_Giro_ID = "GIRO_ID";
            public const String Campo_Tarifa_ID = "TARIFA_ID";
            public const String Campo_Calle_ID = "CALLE_ID";
            public const String Campo_Calle_Referencia1_ID = "CALLE_REFERENCIA1_ID";
            public const String Campo_Calle_Referencia2_ID = "CALLE_REFERENCIA2_ID";
            public const String Campo_Tipo_Vivienda_ID = "TIPO_VIVIENDA_ID";
            public const String Campo_No_Cuenta = "NO_CUENTA";
            public const String Campo_No_Contrato = "NO_CONTRATO";
            public const String Campo_Codigo = "CODIGO";
            public const String Campo_Numero_Exterior = "NUMERO_EXTERIOR";
            public const String Campo_Numero_Interior = "NUMERO_INTERIOR";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Condicion = "CONDICION";
            public const String Campo_Clandestino = "CLANDESTINO";
            public const String Campo_Predio_Constructora = "PREDIO_CONSTRUCTORA";
            public const String Campo_Constructora_ID = "CONSTRUCTORA_ID";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Contratos_Servicios
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_CONTRATOS_SERVICIOS
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 28/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Contratos_Servicios
        {
            public const String Tabla_Cat_Cor_Contratos_Servicios = "CAT_COR_CONTRATOS_SERVICIOS";
            public const String Campo_No_Contrato = "NO_CONTRATO";
            public const String Campo_Numero_Cuenta = "NUMERO_CUENTA";
            public const String Campo_Nombre_Propietario = "NOMBRE_PROPIETARIO";
            public const String Campo_Total = "TOTAL";
            public const String Campo_Tipo_Servicio = "TIPO_SERVICIO";
            public const String Campo_Diametro_Toma = "DIAMETRO_TOMA";
            public const String Campo_Ruta = "RUTA";
            public const String Campo_Sector = "SECTOR";
            public const String Campo_Fecha_Alta = "FECHA_ALTA";
            public const String Campo_Nombre_Gerente_Comercializacion = "NOMBRE_GERENTE_COMERCIALIZACION";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Repartos
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_REPARTOS
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 28/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Repartos
        {
            public const String Tabla_Cat_Cor_Repartos = "CAT_COR_REPARTOS";
            public const String Campo_Reparto_ID = "REPARTO_ID";
            public const String Campo_Numero_Reparto = "NUMERO_REPARTO";
            public const String Campo_Siguiente_Reparto = "SIGUIENTE_REPARTO";
            public const String Campo_Tiempo = "TIEMPO";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Inspectores
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_INSPECTORES
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 30/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Inspectores
        {
            public const String Tabla_Cat_Cor_Inspectores = "CAT_COR_INSPECTORES";
            public const String Campo_Inspector_ID = "INSPECTOR_ID";
            public const String Campo_Empleado_ID = "EMPLEADO_ID";
            public const String Campo_Area_ID = "AREA_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Cajas
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_CAJAS
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 01/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACIÓN: 
        ///*******************************************************************************
        public class Cat_Cor_Cajas
        {
            public const String Tabla_Cat_Cor_Cajas = "CAT_COR_CAJAS";
            public const String Tabla_Cat_Cor_Sucursales = "CAT_COR_SUCURSALES";
            public const String Tabla_Cat_Cor_Turnos = "CAT_COR_TURNOS";
            public const String Campo_Caja_ID = "CAJA_ID";
            public const String Campo_Sucursal_ID = "SUCURSAL_ID";
            public const String Campo_Turno_ID = "TURNO_ID";
            public const String Campo_Numero_Caja = "NUMERO_CAJA";
            public const String Campo_Nombre_Sucursal = "NOMBRE_CAJA";
            public const String Campo_Nombre_Turno = "NOMBRE_TURNO";
            public const String Campo_Responsable = "RESPONSABLE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Motivos_No_Lectura
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_MOTIVOS_NO_LECTURA
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 12/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACIÓN: 
        ///*******************************************************************************
        public class Cat_Cor_Motivos_No_Lectura
        {
            public const String Tabla_Cat_Cor_Motivos_No_Lectura = "CAT_COR_MOTIVOS_NO_LECTURA";
            public const String Campo_Motivo_No_Lectura_ID = "MOTIVO_NO_LECTURA_ID";
            public const String Campo_Clave = "CLAVE";
            public const String Campo_Nombre_Motivo = "NOMBRE_MOTIVO";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Detalles_Servicios_Documentos
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_DETALLES_SERVICIOS_DOCUMENTOS
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 08/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACIÓN: 
        ///*******************************************************************************
        public class Cat_Cor_Detalles_Servicios_Documentos
        {
            public const String Tabla_Cat_Cor_Detalles_Servicios_Documentos = "CAT_COR_DETALLES_SERVICIOS_DOCUMENTOS";
            public const String Campo_Detalles_Servicios_Documentos_ID = "DETALLES_SERVICIOS_DOCUMENTOS_ID";
            public const String Campo_Servicio_ID = "SERVICIO_ID";
            public const String Campo_Documento_ID = "DOCUMENTO_ID";
            public const String Campo_Obligatorio = "OBLIGATORIO";
            public const String Campo_Orden = "ORDEN";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Documentos
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_DOCUMENTOS
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 08/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACIÓN: 
        ///*******************************************************************************
        public class Cat_Cor_Documentos
        {
            public const String Tabla_Cat_Cor_Documentos = "CAT_COR_DOCUMENTOS";
            public const String Campo_Documento_ID = "DOCUMENTO_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Servicios
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_SERVICIOS
        /// PARÁMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 08/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACIÓN: 
        ///*******************************************************************************
        public class Cat_Cor_Servicios
        {
            public const String Tabla_Cat_Cor_Servicios = "CAT_COR_SERVICIOS";
            public const String Campo_Servicio_ID = "SERVICIO_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Tipo = "TIPO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

            //Detalles CAT_COR_SERVICIOS_PREDIOS_DETALLES
            public const String Tabla_Cat_Cor_Servicios_Predios_Detalles = "SERVICIOS_PREDIOS_DETALLES";
            public const String Campo_Predio_ID = "PREDIO_ID";
            public const String Campo_Tipo_Servicio = "TIPO_SERVICIO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Media
        /// DESCRIPCION       : Clase que contiene los campos de la tabla CAT_MEDIA
        /// PARAMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 16/Agosto/2011
        /// MODIFICO          : Josué Daniel Sámano García
        /// FECHA_MODIFICO    : 18/Agosto/2011
        /// CAUSA_MODIFICACION: Se tuvo que cambiar el nombre de Cat_Cor_Imagenes a Cat_Media debido a que era muy en específica y se supone que guarda cualquier tipo de archivo
        ///*******************************************************************************
        public class Cat_Media
        {
            public const String Tabla_Cat_Media = "CAT_MEDIA";
            public const String Campo_Media_ID = "MEDIA_ID";
            public const String Campo_Nombre_Clave = "NOMBRE_CLAVE";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Extension = "EXTENSION";
            public const String Campo_Ruta = "RUTA";
            public const String Campo_MIME = "MIME";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Cor_Solicitudes_Contrato_Servicio
        /// DESCRIPCION       : Clase que contiene los campos de la tabla OPE_COR_SOLICITUDES_CONTRATO_SERVICIO
        /// PARAMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 13/Agosto/2011
        /// MODIFICO          : Josué Daniel Sámano García
        /// FECHA_MODIFICO    : 18/Agosto/2011
        /// CAUSA_MODIFICACION: Modificación de la tabla de OPE_COR_SOLICITUDES_CONTRATO_SERVICIO de la base de datos
        ///*******************************************************************************
        public class Ope_Cor_Solicitudes_Contrato_Servicio
        {
            public const String Tabla_Ope_Cor_Solicitudes_Contrato_Servicio = "OPE_COR_SOLICITUDES_CONTRATO_SERVICIO";
            public const String Campo_No_Solicitud = "NO_SOLICITUD";
            public const String Campo_Folio = "FOLIO";
            public const String Campo_Predio_ID = "PREDIO_ID";
            public const String Campo_Area_ID = "AREA_ID";
            public const String Campo_Nombre_Solicito = "NOMBRE_SOLICITO";
            public const String Campo_Elaboro_ID = "ELABORO_ID";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Fecha_Solicito = "FECHA_SOLICITO";
            public const String Campo_Fecha_Realizo = "FECHA_REALIZO";
            public const String Campo_Duenio = "DUENIO";
            public const String Campo_Condicion = "CONDICION";
            public const String Campo_Condicion_Revision = "CONDICION_REVISION";
            public const String Campo_Tiempo_Habilitado_Deshabilitado = "TIEMPO_HABILITADO_DESHABILITADO";
            public const String Campo_No_Personas = "NO_PERSONAS";
            public const String Campo_Medidor = "MEDIDOR";
            public const String Campo_No_Medidor = "NO_MEDIDOR";
            public const String Campo_Lectura_Medidor = "LECTURA_MEDIDOR";
            public const String Campo_Medida_Largo = "MEDIDA_LARGO";
            public const String Campo_Medida_Ancho = "MEDIDA_ANCHO";
            public const String Campo_Diametro_Toma = "DIAMETRO_TOMA";
            public const String Campo_No_Fotografias = "NO_FOTOGRAFIAS";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Motivo_Cancelacion = "MOTIVO_CANCELACION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

            //Detalles Servicios
            public const String Tabla_Ope_Cor_Solicitudes_Contrato_Servicio_Detalles = "OPE_COR_SOLICITUDES_CONTRATO_SERVICIO_DETALLES";
            public const String Campo_Solicitud_Contrato_Servicio_Detalle_ID = "SOLICITUD_CONTRATO_SERVICIO_DETALLE_ID";
            public const String Campo_Servicio_ID = "SERVICIO_ID";
            public const String Campo_Tipo_Servicio = "TIPO_SERVICIO";
            public const String Campo_Contratado = "CONTRATADO";
            public const String Campo_Cuenta_Servicio = "CUENTA_SERVICIO";
            public const String Campo_Cuenta_Servicio_Revision = "CUENTA_SERVICIO_REVISION";

            //Detalles Documentos
            public const String Tabla_Ope_Cor_Solicitudes_Contrato_Servicio_Documentos_Detalles = "OPE_COR_SOLICITUDES_CONTRATO_SERVICIO_DOCUMENTOS_DETALLES";
            public const String Campo_Solicitud_Contrato_Servicio_Documento_Detalle_ID = "SOLICITUD_CONTRATO_SERVICIO_DOCUMENTO_DETALLE_ID";
            public const String Campo_Documento_ID = "DOCUMENTO_ID";
            public const String Campo_Media_ID = "MEDIA_ID";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Cor_Inspecciones
        /// DESCRIPCION       : Clase que contiene los campos de la tabla OPE_COR_INSPECCIONES
        /// PARAMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 18/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACION: 
        ///*******************************************************************************
        public class Ope_Cor_Inspecciones
        {
            public const String Tabla_Ope_Cor_Inspecciones = "OPE_COR_INSPECCIONES";
            public const String Campo_No_Inspeccion = "NO_INSPECCION";
            public const String Campo_Folio = "FOLIO";
            public const String Campo_No_Solicitud = "NO_SOLICITUD";
            public const String Campo_Predio_ID = "PREDIO_ID";
            public const String Campo_No_Cuenta = "NO_CUENTA";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Asunto = "ASUNTO";
            public const String Campo_Solicito = "SOLICITO";
            public const String Campo_Elaboro_ID = "ELABORO_ID";
            public const String Campo_Fecha_Solicitud = "FECHA_SOLICITUD";
            public const String Campo_Fecha_Realizacion = "FECHA_REALIZACION";
            public const String Campo_Area_ID = "AREA_ID";
            public const String Campo_Inspector_ID = "INSPECTOR_ID";
            public const String Campo_Inspector_Area_ID = "INSPECTOR_AREA_ID";
            public const String Campo_Condicion_Revision = "CONDICION_REVISION";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Motivo_Cancelacion = "MOTIVO_CANCELACION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

            //Detalles
            public const String Tabla_Ope_Cor_Inspecciones_Detalles = "OPE_COR_INSPECCIONES_DETALLES";
            public const String Campo_Inspeccion_Detalle_ID = "INSPECCION_DETALLE_ID";
            public const String Campo_Media_ID = "MEDIA_ID";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Constructoras
        /// DESCRIPCION       : Clase que contiene los campos de la tabla CAT_COR_CONSTRUCTORAS
        /// PARAMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 26/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACION: 
        ///*******************************************************************************
        public class Cat_Cor_Constructoras
        {
            public const String Tabla_Cat_Cor_Constructoras = "CAT_COR_CONSTRUCTORAS";
            public const String Campo_Constructora_ID = "CONSTRUCTORA_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_RFC = "RFC";
            public const String Campo_Razon_Social = "RAZON_SOCIAL";
            public const String Campo_Estado_ID = "ESTADO_ID";
            public const String Campo_Ciudad_ID = "CIUDAD_ID";
            public const String Campo_Colonia = "COLONIA";
            public const String Campo_Calle = "CALLE";
            public const String Campo_No_Interior = "NO_INTERIOR";
            public const String Campo_No_Exterior = "NO_EXTERIOR";
            public const String Campo_Contacto = "CONTACTO";
            public const String Campo_Email = "EMAIL";
            public const String Campo_Tel = "TEL";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Cor_Contratos
        /// DESCRIPCION       : Clase que contiene los campos de la tabla OPE_COR_CONTRATOS
        /// PARAMETROS        :     
        /// CREO              : Josué Daniel Sámano García
        /// FECHA_CREO        : 26/Agosto/2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACION: 
        ///*******************************************************************************
        public class Ope_Cor_Contratos
        {
            public const String Tabla_Ope_Cor_Contratos = "OPE_COR_CONTRATOS";
            public const String Campo_No_Contrato = "NO_CONTRATO";
            public const String Campo_Folio = "FOLIO";
            public const String Campo_No_Cuenta = "NO_CUENTA";
            public const String Campo_Usuario_ID = "USUARIO_ID";
            public const String Campo_Predio_ID = "PREDIO_ID";
            public const String Campo_No_Solicitud = "NO_SOLICITUD";
            public const String Campo_Tarifa_ID = "TARIFA_ID";
            public const String Campo_Fecha_Contrato = "FECHA_CONTRATO";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

            public const String Campo_Padron_Usuario = "NO_PADRON_USUARIO";
        }    


        ///*******************************************************************************
        /// NOMBRE DE LA FUNCION: Cat_Cor_Colonias
        /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_COR_COLONIAS
        /// PARAMETROS :
        /// CREO       : Raúl Raya Ortega
        /// FECHA_CREO : 16/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACION:
        ///*******************************************************************************
        public class Cat_Cor_Colonias
        {
            public const String Tabla_Cat_Cor_Colonias = "CAT_COR_COLONIAS";
            public const String Campo_Colonia_ID = "COLONIA_ID";
            public const String Campo_Tipo_Colonia_Id = "TIPO_COLONIA_ID";
            public const String Campo_Zona_ID = "ZONA_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Codigo_Postal = "CODIGO_POSTAL";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Distrito_ID = "Distrito_ID";
            public const String Campo_Region_ID = "Region_ID";
            public const String Campo_Poblacion_ID = "Poblacion_id";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA FUNCION: Cat_Cor_Zonas
        /// DESCRIPCION: Clase con contiene los datos de la tabla CAT_COR_ZONAS
        /// PARAMETROS :
        /// CREO       : Raúl Raya Ortega
        /// FECHA_CREO : 20/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACION:
        ///*******************************************************************************
        public class Cat_Cor_Zonas
        {
            public const String Tabla_Cat_Cor_Zonas = "CAT_COR_ZONAS";
            public const String Campo_Zona_Id = "ZONA_ID";
            public const String Campo_Numero_Zona = "NUMERO_ZONA";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Region_ID = "REGION_ID";

        }
        

        
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Giros
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_GIROS
        /// PARÁMETROS        :     
        /// CREO              : Raúl Raya Ortaga
        /// FECHA_CREO        : 23/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Giros
        {
            public const String Tabla_Cat_Cor_Giros = "CAT_COR_GIROS";
            public const String Campo_Giro_ID = "GIRO_ID";
            public const String Campo_Nombre_Giro = "NOMBRE_GIRO";
            public const String Campo_Clasificacion_Giro_ID = "CLASIFICACION_GIROS_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Comentarios = "COMENTARIOS";

        }
        
        
        ///*******************************************************************************
        /// NOMBRE DE LA FUNCION: Cat_Cor_Usuarios
        /// DESCRIPCION:        Clase con contiene los datos de la tabla CAT_COR_USUARIOS
        /// PARAMETROS :
        /// CREO       :        Raúl Raya Ortega
        /// FECHA_CREO :        28/Julio/2011
        /// MODIFICO          : Diego N. Yañez Rodriguez
        /// FECHA_MODIFICO    : 01 Septiembre 2011
        /// CAUSA_MODIFICACION: Se agregó mas información a la tabla
        ///*******************************************************************************
        public class Cat_Cor_Usuarios
        {
            public const String Tabla_Cat_Cor_Usuarios = "CAT_COR_USUARIOS";
            public const String Campo_Usuario_Id = "USUARIO_ID";
            public const String Campo_Estado_ID = "ESTADO_ID";
            public const String Campo_Ciudad_ID = "CIUDAD_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Apellido_Paterno = "APELLIDO_PATERNO";
            public const String Campo_Apellido_Materno = "APELLIDO_MATERNO";
            public const String Campo_Rfc = "RFC";
            public const String Campo_Razon_Social = "RAZON_SOCIAL";
            public const String Campo_Calle = "CALLE";
            public const String Campo_Colonia = "COLONIA";
            public const String Campo_Calle_Id = "CALLE_ID";
            public const String Campo_Colonia_Id = "COLONIA_ID";
            public const String Campo_Pais = "PAIS";
            public const String Campo_No_Interior = "NO_INTERIOR";
            public const String Campo_No_Exterior = "NO_EXTERIOR";
            public const String Campo_Telefono_Casa = "TELEFONO_CASA";
            public const String Campo_Telefono_Oficina = "TELEFONO_OFICINA";
            public const String Campo_Telefono_Celular = "TELEFONO_CELULAR";
            public const String Campo_Telefono_Nextel = "NEXTEL";
            public const String Campo_Correo_Electronico = "CORREO_ELECTRONICO";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Folio_IFE = "FOLIO_IFE";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA FUNCION: CAT_COR_USUARIOS_PREDIOS_DETALLES
        /// DESCRIPCION:        Clase con contiene los datos de la tabla CAT_COR_USUARIOS_PREDIOS_DETALLES
        /// PARAMETROS :
        /// CREO       :        Diego N. Yañez Rodriguez
        /// FECHA_CREO :        01 Septiembre 2011
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACION: 
        ///*******************************************************************************
        public class Cat_Cor_Usuarios_Predios_Detalles
        {
            public const String Tabla_Cat_Cor_Usuarios_Predios_Detalles = "CAT_COR_USUARIOS_PREDIOS_DETALLES";
            public const String Campo_Usuario_Id = "USUARIO_ID";
            public const String Campo_Predio_Id = "PREDIO_ID";
            public const String Campo_Usuario = "USUARIO";
            public const String Campo_Password = "PASSWORD";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Colonias_Detalles
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_COLONIAS_DETALLES
        /// PARÁMETROS        :     
        /// CREO              : Raúl Raya Ortega
        /// FECHA_CREO        : 01/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Colonias_Detalles
        {
            public const String Tabla_Cat_Cor_Colonias_Detalles = "[CAT_COR_COLONIAS_DETALLES]";
            public const String Campo_Colonia_Detalle_ID = "DETALLE_COL_ID";
            public const String Campo_Colonia_ID = "COLONIA_ID";
            public const String Campo_Calle_ID = "CALLE_ID";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Almacenes
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_ALMACENES
        /// PARÁMETROS        :     
        /// CREO              : Raúl_Raya Ortega
        /// FECHA_CREO        : 01/Agosto/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Almacenes
        {
            public const String Tabla_Cat_Cor_Almacenes = "CAT_COR_ALMACENES";
            public const String Campo_Almacen_ID = "ALMACEN_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Departamento_ID = "DEPARTAMENTO_ID";
            public const String Campo_Sucursal_ID = "SUCURSAL_ID";
            public const String Campo_Responsable = "RESPONSABLE";
            public const String Campo_Numero_Almacen = "NUMERO_ALMACEN";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Cor_Departamentos
        /// DESCRIPCIÓN       : Clase que contiene los campos de la tabla CAT_COR_DEPARTAMENTOS
        /// PARÁMETROS        :     
        /// CREO              : Raúl_Raya Ortega
        /// FECHA_CREO        : 01/Agosto/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Cor_Departamentos
        {
            public const String Tabla_Cat_Cor_Almacenes = "CAT_COR_DEPARTAMENTOS";
            public const String Campo_Almacen_ID = "DEPARTAMENTO_ID";
            public const String Campo_Nombre_Departamento = "NOMBRE_DEPARTAMENTO";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_no_Cuenta = "NO_CUENTA";
            public const String Campo_Sub_Cuenta_ID = "SUB_CUENTA_ID";
            public const String Campo_Sub_Sub_Cuenta_ID = "SUB_SUB_CUENTA_ID";
            public const String Campo_Sub_Sub_Sub_Cuenta_ID = "SUB_SUB_SUB_CUENTA_ID";
            public const String Campo_Area_ID = "AREA_ID";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Cat_Cor_Tipos_Viviendas
        /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_Cor_Tipos_Viviendas
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           12/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Cat_Cor_Tipos_Viviendas
        {
            public const String Tabla_Cat_Cor_Tipos_Viviendas = "CAT_Cor_Tipos_Viviendas";
            public const String Campo_Tipo_Vivienda_ID = "TIPO_VIVIENDA_ID";
            public const String Campo_Tipo = "TIPO";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE : Cat_Cor_Tipos_Colonias
        /// DESCRIPCIÓN        : Clase que contiene los campos de la tabla CAT_COR_TIPOS_COLONIAS
        /// PARÁMETROS         : 
        /// CREO               : Antonio Salvador Benavides Guardado
        /// FECHA_CREO         : 27/Octubre/2010 
        /// MODIFICO           :
        /// FECHA_MODIFICO     :
        /// CAUSA_MODIFICACIÓN :
        ///*******************************************************************************
        public class Cat_Cor_Tipos_Colonias
        {
            public const String Tabla_Cat_Cor_Tipos_Colonias = "CAT_COR_TIPOS_COLONIAS";
            public const String Campo_Tipo_Colonia_ID = "TIPO_COLONIA_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Cat_Cor_Condiciones_Viviendas
        /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COR_CONDICIONES_VIVIENDAS
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           12/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Cat_Cor_Condiciones_Viviendas
        {
            public const String Tabla_Cat_Cor_Condiciones_Viviendas = "CAT_COR_CONDICIONES_VIVIENDAS";
            public const String Campo_Condiciones_Vivienda_ID = "CONDICIONES_VIVIENDA_ID";
            public const String Campo_Condicion = "CONDICION";
            public const String Campo_Concepto = "CONCEPTO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

        }
        /////*******************************************************************************
        ///// NOMBRE DE LA CLASE:    Cat_Cor_Servicios_Viviendas
        ///// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_Cor_Servicios_Viviendas
        ///// PARAMETROS :     
        ///// CREO       :           Raul_Raya_Ortega
        ///// FECHA_CREO :           12/Agosto/2011 
        ///// MODIFICO          :     
        ///// FECHA_MODIFICO    :    
        ///// CAUSA_MODIFICACION:    
        /////*******************************************************************************/
        //public class Cat_Cor_Servicios_Viviendas
        //{
        //    public const String Tabla_Cat_Cor_Servicios_Viviendas = "CAT_Cor_Servicios_Viviendas";
        //    public const String Campo_Servicio_Vivienda_ID = "SERVICIO_VIVIENDA_ID";
        //    public const String Campo_Servicio = "SERVICIO";
        //    public const String Campo_Descripcion = "DESCRIPCION";
        //    public const String Campo_Usuario_Creo = "USUARIO_CREO";
        //    public const String Campo_Fecha_Creo = "FECHA_CREO";
        //    public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        //    public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        //}
        /*******************************************************************************
        NOMBRE DE LA CLASE: Cat_Cor_Motivos_Solicitud
        DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_COR_MOTIVOS_SOLICITUD
        PARÁMETROS :     
        CREO       : RAÚL RAYA ORTEGA1
        FECHA_CREO : 8/Agosto/2011 
        MODIFICO          :
        FECHA_MODIFICO    :
        CAUSA_MODIFICACIÓN:
       *******************************************************************************/
        public class Cat_Cor_Motivos_Solicitud
        {
            public const String Tabla_Cat_Cor_Motivos_Solicitud = "CAT_COR_MOTIVOS_SOLICITUD";
            public const String Campo_Motivo_ID = "MOTIVO_ID";
            public const String Campo_Motivo = "MOTIVO";
            public const String Campo_Tipo = "TIPO";
            public const String Campo_Soporte = "SOPORTE";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";


        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Cat_Cor_Conceptos_Ingresos_Egresos_Usuarios
        /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COR_Conceptos_Ingresos_Egresos_Usuarios
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           12/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Cat_Cor_Conceptos_Ingresos_Egresos_Usuarios
        {
            public const String Tabla_Cat_Cor_Conceptos_Ingresos_Egresos_Usuarios = "CAT_COR_Conceptos_Ingresos_Egresos_Usuarios";
            public const String Campo_Concepto_ID = "CONCEPTO_ID";
            public const String Campo_Tipo = "TIPO";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Ope_Cor_Solicitudes_Estudio_Socioeconomico
        /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COR_SOLICITUDES_ESTUDIO_SOCIOECONOMICO
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           17/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Ope_Cor_Solicitudes_Estudio_Socioeconomico
        {
            public const String Tabla_Ope_Cor_Solicitudes_Estudio_Socioeconomico = "OPE_COR_SOLICITUDES_ESTUDIO_SOCIOECONOMICO";
            public const String Campo_No_Solicitud = "NO_SOLICITUD";
            public const String Campo_Folio = "FOLIO";
            public const String Campo_Solicitada_Por="SOLICITADA_POR";
            public const String Campo_Predio_ID = "PREDIO_ID";            
            public const String Campo_Fecha_Solicitud = "FECHA_SOLICITUD";
            public const String Campo_Fecha_Realizo = "FECHA_REALIZO";
            public const String Campo_Motivo_ID = "MOTIVO_ID";
            public const String Campo_Area_Empleado_Elaboro_ID = "AREA_EMPLEADO_ELABORO_ID";
            public const String Campo_Area_Empleado_Asignado_ID = "AREA_EMPLEADO_ASIGNADO_ID";
            public const String Campo_Empleado_Elaboro_ID = "EMPLEADO_ELABORO_ID";
            public const String Campo_Empleado_Asignado_ID = "EMPLEADO_ASIGNADO_ID";
            public const String Campo_Firma_Solicitante_Electronico = "FIRMA_SOLICITANTE_ELECTRONICO";
            public const String Campo_Firma_Solicitante_Digital = "FIRMA_SOLICITANTE_DIGITAL";
            public const String Campo_Croquis_Ubicacion = "CROQUIS_UBICACION";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";


        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Ope_Cor_Estudios_Socioeconomicos
        /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COR_ESTUDIOS_SOCIOECONOMICOS
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           17/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Ope_Cor_Estudios_Socioeconomicos
        {
            public const String Tabla_Ope_Cor_Estudios_Socioeconomicos = "OPE_COR_ESTUDIOS_SOCIOECONOMICOS";
            public const String Campo_No_Estudio = "NO_ESTUDIO";
            public const String Campo_No_Solicitud = "NO_SOLICITUD";
            public const String Campo_Empleado_ID = "EMPLEADO_ID";
            public const String Campo_Folio = "FOLIO";
            public const String Campo_Fecha = "FECHA";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Ocupacion_Solicitante = "OCUPACION_SOLICITANTE";
            public const String Campo_Estado_Civil = "ESTADO_CIVIL";
            public const String Campo_Edad = "EDAD";
            public const String Campo_Sexo = "SEXO";
            public const String Campo_Ingresos = "INGRESOS";
            public const String Campo_Egresos = "EGRESOS";
            public const String Campo_Tipo_Vivienda_ID = "TIPO_VIVIENDA_ID";
            public const String Campo_Propiedad_Vivienda = "PROPIEDAD_VIVIENDA";
            public const String Campo_Condiciones_Vivienda_ID_Techo = "CONDICIONES_VIVIENDA_ID_TECHO";
            public const String Campo_Condiciones_Vivienda_ID_Paredes = "CONDICIONES_VIVIENDA_ID_PAREDES";
            public const String Campo_Condiciones_Vivienda_ID_Piso = "CONDICIONES_VIVIENDA_ID_PISO";
            public const String Campo_No_Cuartos = "NUMERO_CUARTOS";
            public const String Campo_Llave_Publica = "LLAVE_PUBLICA";
            public const String Campo_Linea_General = "LINEA_GENERAL";
            public const String Campo_Pipas = "PIPAS";
            public const String Campo_Regularidad_Servicio_Agua = "REGULARIDAD_SERVICIO_AGUA";
            public const String Campo_Regularidad_Servicio_Drenaje = "REGULARIDAD_SERVICIO_DRENAJE";
            public const String Campo_Comentarios_Usuario = "COMENTARIOS_USUARIO";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Firma_Solicitante_Electronica = "FIRMA_SOLICITANTE_ELECTRONICA";
            public const String Campo_Firma_Solicitante_Digital = "FIRMA_SOLICITANTE_DIGITAL";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_COSINA_SEPARADA = "COSINA_SEPARADA";
            public const String Campo_BAÑO_PUBLICO = "BAÑO_PUBLICO";
            public const String Campo_LETRINA = "LETRINA";
            public const String Campo_FOSA_SEPTICA = "FOSA_SEPTICA";
            public const String Campo_AIRE_LIBRE = "AIRE_LIBRE";
            public const String Campo_BAÑOS_WC = "BAÑOS_WC";

        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Ope_Cor_Estudio_Socioeconomico_Detalles_Familia
        /// DESCRIPCION:           Clase que contiene los campos de la tabla [OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_FAMILIA]
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           19/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Ope_Cor_Estudio_Socioeconomico_Detalles_Familia
        {
            public const String Tabla_Ope_Cor_Estudio_Socioeconomico_Detalles_Familia = "OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_FAMILIA";
            public const String Campo_No_Detalle = "NO_DETALLE";
            public const String Campo_No_Estudio = "NO_ESTUDIO";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Edad = "EDAD";
            public const String Campo_Parentezco = "PARENTEZCO";
            public const String Campo_Ocupacion = "OCUPACION";
            public const String Campo_Escolaridad_ID = "ESCOLARIDAD_ID";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Cat_Cor_Tipos_Cuotas
        /// DESCRIPCION:           Clase que contiene los campos de la tabla Cat_Cor_Tipos_Cuotas
        /// PARAMETROS :     
        /// CREO       :           Diego N. Yañez
        /// FECHA_CREO :           19/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Cat_Cor_Tipos_Cuotas
        {
            public const String Tabla_Cat_Cor_Tipos_Cuotas = "CAT_COR_TIPOS_CUOTAS";
            public const String Campo_Cuota_ID = "CUOTA_ID";
            public const String Campo_Descripcion ="DESCRIPCION";
            public const String Campo_Clave = "CLAVE";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Cat_Cor_Tarifas
        /// DESCRIPCION:           Clase que contiene los campos de la tabla Cat_Cor_Tarifas
        /// PARAMETROS :     
        /// CREO       :           Diego N. Yañez
        /// FECHA_CREO :           19/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Cat_Cor_Tarifas
        {
            public const String Tabla_Cat_Cor_Tarifas = "CAT_COR_TARIFAS";
            public const String Campo_Tarifa_ID = "TARIFA_ID";
            public const String Campo_Cuota_ID = "CUOTA_ID";
            public const String Campo_Giro_ID = "Giro_ID";
            public const String Campo_Zona_ID = "Zona_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Abreviatura = "ABREVIATURA";
            public const String Campo_Clave = "CLAVE";
            public const String Campo_Rango_Inicial = "RANGO_INICIAL";
            public const String Campo_Rango_Final = "RANGO_FINAL";
            public const String Campo_Aplicacion_Tarifa = "APLICACION_TARIFA";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    CAT_COR_TARIFAS_ANUALES
        /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COR_TARIFAS_ANUALES
        /// PARAMETROS :     
        /// CREO       :           Diego N. Yañez
        /// FECHA_CREO :           19/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Cat_Cor_Tarifas_Anuales
        {
            public const String Tabla_Cat_Cor_Tarifas_Anuales = "CAT_COR_TARIFAS_ANUALES";
            public const String Campo_Tarifa_Anual_ID = "TARIFA_ANUAL_ID";
            public const String Campo_Tarifa_ID = "TARIFA_ID";
            public const String Campo_Mes = "MES";
            public const String Campo_Anio = "ANIO";
            public const String Campo_Cuota_Base = "CUOTA_BASE";
            public const String Campo_Cuota_Excedente = "CUOTA_EXCEDENTE";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    CAT_COR_TARIFAS_ANUALES_DETALLES
        /// DESCRIPCION:           Clase que contiene los campos de la tabla CAT_COR_TARIFAS_ANUALES
        /// PARAMETROS :     
        /// CREO       :           Diego N. Yañez
        /// FECHA_CREO :           19/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        //public class CAT_COR_TARIFAS_ANUALES_DETALLES
        //{
        //    public const String Tabla_Cat_Cor_Tarifas_Anuales_Detalles = "CAT_COR_TARIFAS_ANUALES_DETALLES";
        //    public const String Campo_Consecutivo = "CONSECUTIVO";
        //    public const String Campo_Tarifa_Anual_ID = "TARIFA_ANUAL_ID";
        //    public const String Campo_Consumo = "CONSUMO";
        //    public const String Campo_Tarifa = "TARIFA";

        //}

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Ope_Cor_Estudio_Socioeconomico_Detalles_Servicios_Viviendas
        /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_SERVICIOS_VIVIENDAS
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           19/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Ope_Cor_Estudio_Socioeconomico_Detalles_Servicios_Viviendas
        {
            public const String Tabla_Ope_Cor_Estudio_Socioeconomico_Detalles_Servicios_Viviendas = "OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_SERVICIOS_VIVIENDAS";
            public const String Campo_No_Detalle = "NO_DETALLE";
            public const String Campo_No_Estudio = "NO_ESTUDIO";
            public const String Campo_Servicio_ID = "SERVICIO_ID";

        }
            ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Ope_Cor_Estudio_Socioeconomico_Detalles_Condiciones_Vivienda
        /// DESCRIPCION:           Clase que contiene los campos de la tabla OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_CONDICIONES_VIVIENDA
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           29/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Ope_Cor_Estudio_Socioeconomico_Detalles_Condiciones_Vivienda
        {
            public const String Tabla_Ope_Cor_Estudio_Socioeconomico_Detalles_Condiciones_Vivienda = "OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_CONDICIONES_VIVIENDA";
            public const String Campo_No_Detalle = "NO_DETALLE";
            public const String Campo_No_Estudio = "NO_ESTUDIO";
            public const String Campo_Condiciones_Vivienda_ID = "CONDICIONES_VIVIENDA_ID";

        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Ope_Cor_Estudio_Socioeconomico_Detalles_Ingresos
        /// DESCRIPCION:           Clase que contiene los campos de la tabla [OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_INGRESOS]
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           19/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Ope_Cor_Estudio_Socioeconomico_Detalles_Ingresos
        {
            public const String Tabla_Ope_Cor_Estudio_Socioeconomico_Detalles_Ingresos = "OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_INGRESOS";
            public const String Campo_No_Detalle = "NO_DETALLE";
            public const String Campo_No_Estudio = "NO_ESTUDIO";
            public const String Campo_Concepto_ID = "CONCEPTO_ID";
            public const String Campo_Ingreso = "INGRESO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE:    Ope_Cor_Estudio_Socioeconomico_Detalles_Egresos
        /// DESCRIPCION:           Clase que contiene los campos de la tabla [OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_EGRESOS]
        /// PARAMETROS :     
        /// CREO       :           Raul_Raya_Ortega
        /// FECHA_CREO :           19/Agosto/2011 
        /// MODIFICO          :     
        /// FECHA_MODIFICO    :    
        /// CAUSA_MODIFICACION:    
        ///*******************************************************************************/
        public class Ope_Cor_Estudio_Socioeconomico_Detalles_Egresos
        {
            public const String Tabla_Ope_Cor_Estudio_Socioeconomico_Detalles_Egresos = "OPE_COR_ESTUDIO_SOCIOECONOMICO_DETALLES_EGRESOS";
            public const String Campo_No_Detalle = "NO_DETALLE";
            public const String Campo_No_Estudio = "NO_ESTUDIO";
            public const String Campo_Concepto_ID = "CONCEPTO_ID";
            public const String Campo_Egreso = "CANTIDAD";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }



        ///*******************************************************************************
        /// NOMBRE DE LA CLASE      : Cat_Cor_Conceptos_Cobros
        /// DESCRIPCION             : Clase que contiene los campos de la tabla Cat_Cor_Conceptos_Cobros
        /// PARAMETROS              :     
        /// CREO                    : Julio Cruz
        /// FECHA_CREO              : 20/Sep/2011 
        /// MODIFICO                :     
        /// FECHA_MODIFICO          :    
        /// CAUSA_MODIFICACION      :    
        ///*******************************************************************************/
        public class Cat_Cor_Conceptos_Cobros
        {
            public const String Tabla_Cat_Cor_Conceptos_Cobros = "CAT_COR_CONCEPTOS_COBROS";
            public const String Campo_Concepto_ID = "CONCEPTO_ID";
            public const String Campo_Tipo_Calculo= "TIPO_CALCULO";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Giro_Actividad = "GIRO_ACTIVIDAD_ID";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Prioridad = "Prioridad";
            public const String Campo_Cuenta_Contable_ID = "CUENTA_CONTABLE_ID";
            public const String Campo_Giro_Actividad_ID = "GIRO_ACTIVIDAD_ID";
            public const String Campo_Estatus= "Estatus";
            public const String Campo_Facturable = "Facturable";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA FUNCION: Cat_Cor_Predios_Usuarios
        /// DESCRIPCION:        Clase con contiene los datos de la tabla Cat_Cor_Predios_Usuarios
        /// PARAMETROS :
        /// CREO       :        Julio Cruz
        /// FECHA_CREO :        01 30 2012
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACION: 
        ///*******************************************************************************
        public class Cat_Cor_Predios_Usuarios
        {
            public const String Tabla_Cat_Cor_Predios_Usuarios = "Cat_Cor_Predios_Usuarios";
            public const String Campo_Usuario_Id = "USUARIO_ID";
            public const String Campo_Predio_ID = "Predio_ID";
            public const String Campo_Giro_ID = "Giro_ID";
            public const String Campo_Giro_Actividad_ID = "Giro_Actividad_ID";
            public const String Campo_Grupo_Concepto_Cobro_ID = "Grupo_Concepto_Cobro_ID";
            public const String Campo_No_Cuenta = "No_Cuenta";
            public const String Campo_Usuario = "Usuario";
            public const String Campo_Password = "Password";
            public const String Campo_Estatus = "Estatus";
            public const String Campo_Periodo = "Periodo";
            public const String Campo_Fecha_Inicio_Servicio = "Fecha_Inicio_Servicio";
            public const String Campo_Prefijo_Actividad = "Prefijo_Actividad";
            public const String Campo_Metodo_Facturacion = "Metodo_Facturacion";
            public const String Campo_Pozo_Propio = "Pozo_Propio";
            public const String Campo_Tipo_Laboratorio = "Tipo_Laboratorio";
            public const String Campo_Planta_Tratamiento = "Planta_Tratamiento";
        }



        ///*******************************************************************************
        /// NOMBRE DE LA FUNCION: Cat_Cor_Descuentos_Anios
        /// DESCRIPCION:        Clase con contiene los datos de la tabla Cat_Cor_Descuentos_Anios 
        /// PARAMETROS :
        /// CREO       :        Alejandro Leyva Alvarado
        /// FECHA_CREO :        9-sep-2013
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACION: 
        ///*******************************************************************************
        public class Cat_Cor_Descuentos_Anios 
        {
            public const String Tabla_Cat_Cor_Descuentos_Anios = "Cat_Cor_Descuentos_Anios ";
            public const String Campo_Descuento_Id = "Descuento_Id";
            public const String Campo_Nombre_Descuento = "Nombre_Descuento";
            public const String Campo_Descripcion_Descuento = "Descripcion_Descuento";
            public const String Campo_Fecha_Inicio = "Fecha_Inicio";
            public const String Campo_Fecha_Termino = "Fecha_Termino";
            public const String Campo_Prioridad = "Prioridad";
            public const String Campo_Porcentaje_Descuento = "Porcentaje_Descuento";
            public const String Campo_Cantidad_Descuento = "Cantidad_Descuento";
            public const String Campo_Autorizacion = "Autorizacion";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Concepto_Afecta = "Concepto_Afecta";
            public const String Campo_Clasificacion= "Clasificacion";
        }
        /// <summary>
        /// NOMBRE DE LA FUNCION: Cat_Cor_Liquidacion_Contratos
        /// DESCRIPCION         : Clase con contiene los datos de la tabla CAT_COR_LIQUIDACION_CONTRATOS 
        /// CREO                : Tania Yerena Juárez Hernández
        /// FECHA_CREO          : 26-Octubre-2013
        /// MODIFICO            : 
        /// FECHA_MODIFICO      : 
        /// CAUSA_MODIFICACION  : 
        /// </summary>
        public class Cat_Cor_Liquidacion_Contratos
        {
            public const String Tabla_Cat_Cor_Liquidacion_Contratos = "CAT_COR_LIQUIDACION_CONTRATOS";
            public const String Campo_Liquidacion_ID = "LIQUIDACION_ID";
            public const String Campo_No_Solicitud = "NO_SOLICITUD";
            public const String Campo_Usuario_ID = "USUARIO_ID";
            public const String Campo_Predio_ID = "PREDIO_ID";
            public const String Campo_Fecha_Elaboro = "FECHA_ELABORO";
            public const String Campo_Fecha_Vigencia = "FECHA_VIGENCIA";
            public const String Campo_Total = "TOTAL";
            public const String Campo_SubTotal = "SUBTOTAL";
            public const String Campo_IVA = "IVA";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        /// <summary>
        /// NOMBRE DE LA FUNCION: Cat_Cor_Liquidacion_Contratos_Detalles
        /// DESCRIPCION         : Clase con contiene los datos de la tabla CAT_COR_LIQUIDACION_CONTRATOS_DETALLES 
        /// PARAMETROS          : Tania Yerena Juárez Hernández
        /// FECHA_CREO          : 26-Octubre-2013
        /// MODIFICO            : 
        /// FECHA_MODIFICO      : 
        /// CAUSA_MODIFICACION  : 
        /// </summary>
        public class Cat_Cor_Liquidacion_Contratos_Detalles
        {
            public const String Tabla_Cat_Cor_Liquidacion_Contratos_Detalles = "CAT_COR_LIQUIDACION_CONTRATOS_DETALLES";
            public const String Campo_Liquidacion_Detalle_ID = "LIQUIDACION_DETALLE_ID";
            public const String Campo_Liquidacion_ID = "LIQUIDACION_ID";
            public const String Campo_Concepto_ID = "CONCEPTO_ID";
            public const String Campo_Importe = "IMPORTE";
            public const String Campo_IVA = "IVA";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

        }

        /// NOMBRE DE LA FUNCION: Cat_Cor_Liquidacion_Contratos_Detalles_Desglose
        /// DESCRIPCION         : Clase contiene los datos de la tabla CAT_COR_LIQUIDACION_CONTRATOS_DESGLOSE_DETALLES 
        /// PARAMETROS          : Julio Cruz
        /// FECHA_CREO          : 02-Febrero-2014
        /// MODIFICO            : 
        /// FECHA_MODIFICO      : 
        /// CAUSA_MODIFICACION  : 
        /// </summary>
        public class Cat_Cor_Liquidacion_Contratos_Detalles_Desglose
        {
            public const String Tabla_Cat_Cor_Liquidacion_Contratos_Detalles = "CAT_COR_LIQUIDACION_CONTRATOS_DESGLOSE_DETALLES";
            public const String Campo_Liquidacion_Detalle_ID = "LIQUIDACION_DETALLE_ID";
            public const String Campo_Liquidacion_ID = "LIQUIDACION_ID";
            public const String Campo_Concepto_ID = "CONCEPTO_ID";
            public const String Campo_Importe = "IMPORTE";
            public const String Campo_IVA = "IVA";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

        }
        /// NOMBRE DE LA FUNCION: Ope_Cor_Visitas
        /// DESCRIPCION         : Clase contiene los datos de la tabla Ope_Cor_Visitas 
        /// PARAMETROS          : José Maldonado Méndez 
        /// FECHA_CREO          : 07/Abril/2016
        /// MODIFICO            : 
        /// FECHA_MODIFICO      : 
        /// CAUSA_MODIFICACION  : 
        /// </summary>
        public class Ope_Cor_Visitas
        {
            public const String Tabla_Ope_Cor_Visitas = "Ope_Cor_Visitas";
            public const String Campo_No_Visita = "No_Visita";
            public const String Campo_No_Solicitud = "No_Solicitud";
            public const String Campo_Fecha_Visita = "Fecha_Visita";
            public const String Campo_Observaciones = "Observaciones";
            public const String Campo_Usuario_Creo = "Usuario_Creo";
            public const String Campo_Fecha_Creo = "Fecha_Creo";
            public const String Campo_Usuario_Modifico = "Usuario_Modifico";
            public const String Campo_Fecha_Modifico = "Fecha_Modifico";

        }

    #endregion

        ///*************************************************************************************************************************
        ///                                                                SubRegion Control Patrimonial
        ///*************************************************************************************************************************
        #region SubRegion_Control_Patrimonial
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Clases_Activo
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_CLASES_ACTIVO
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Enero/2012 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Clases_Activo
        {
            public const String Tabla_Cat_Pat_Clases_Activo = "CAT_PAT_CLASES_ACTIVO";
            public const String Campo_Clase_Activo_ID = "CLASE_ACTIVO_ID";
            public const String Campo_Clave = "CLAVE";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Anios_Vida_Util = "ANIOS_VIDA_UTIL";
            public const String Campo_Porcentaje_Depreciacion = "PORCENTAJE_DEPRECIACION";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Tabla_Ope_Pat_Bienes_Sin_Inv
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_BIENES_SIN_INV
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 03/Noviembre/2011 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///********************************************************************************
        public class Ope_Pat_Bienes_Sin_Inv
        {
            public const String Tabla_Ope_Pat_Bienes_Sin_Inv = "OPE_PAT_BIENES_SIN_INV";
            public const String Campo_Bien_ID = "BIEN_ID";
            public const String Campo_Bien_Parent_ID = "BIEN_PARENT_ID";
            public const String Campo_Tipo_Parent = "TIPO_PARENT";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Marca_ID = "MARCA_ID";
            public const String Campo_Costo_Inicial = "COSTO_INICIAL";
            public const String Campo_Material_ID = "MATERIAL_ID";
            public const String Campo_Color_ID = "COLOR_ID";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Motivo_Baja = "MOTIVO_BAJA";
            public const String Campo_Fecha_Adquisicion = "FECHA_ADQUISICION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Producto_ID = "PRODUCTO_ID";
            public const String Campo_Modelo = "MODELO";
            public const String Campo_Numero_Serie = "NUMERO_SERIE";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Zonas
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_ZONAS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Zonas
        {
            public const String Tabla_Cat_Pat_Zonas = "CAT_PAT_ZONAS";
            public const String Campo_Zona_ID = "ZONA_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }



        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Clasificaciones
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_CLASIFICACIONES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Clasificaciones
        {
            public const String Tabla_Cat_Pat_Clasificaciones = "CAT_PAT_CLASIFICACIONES";
            public const String Campo_Clasificacion_ID = "CLASIFICACION_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Clave = "CLAVE";
            public const String Campo_Anios_Vida_Util = "ANIOS_VIDA_UTIL";
            public const String Campo_Porcentaje_Depreciacion = "PORCENTAJE_DEPRECIACION";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipos_Bajas
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPOS_BAJAS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipos_Bajas
        {
            public const String Tabla_Cat_Pat_Tipos_Bajas = "CAT_PAT_TIPOS_BAJAS";
            public const String Campo_Tipo_Baja_ID = "TIPO_BAJA_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }



        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipos_Vehiculo
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPOS_VEHICULO
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Noviembre/2010 
        /// MODIFICO          :Franciso Antonio Gallardo Castañeda
        /// FECHA_MODIFICO    :11/Marzo/2011
        /// CAUSA_MODIFICACIÓN:Se le agrego la parte de Aseguradora a el tipo de Vehículo.
        ///*******************************************************************************
        public class Cat_Pat_Tipos_Vehiculo
        {
            public const String Tabla_Cat_Pat_Tipos_Vehiculo = "CAT_PAT_TIPOS_VEHICULO";
            public const String Campo_Tipo_Vehiculo_ID = "TIPO_VEHICULO_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Aseguradora_ID = "ASEGURADORA_ID";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipos_Combustible
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPOS_COMBUSTIBLE
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipos_Combustible
        {
            public const String Tabla_Cat_Pat_Tipos_Combustible = "CAT_PAT_TIPOS_COMBUSTIBLE";
            public const String Campo_Tipo_Combustible_ID = "TIPO_COMBUSTIBLE_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Colores
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_COLORES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Colores
        {
            public const String Tabla_Cat_Pat_Colores = "CAT_PAT_COLORES";
            public const String Campo_Color_ID = "COLOR_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Materiales
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_MATERIALES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 24/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Materiales
        {
            public const String Tabla_Cat_Pat_Materiales = "CAT_PAT_MATERIALES";
            public const String Campo_Material_ID = "MATERIAL_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Aseguradora
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_ASEGURADORAS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 24/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Aseguradora
        {
            public const String Tabla_Cat_Pat_Aseguradora = "CAT_PAT_ASEGURADORAS";
            public const String Campo_Aseguradora_ID = "ASEGURADORA_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Nombre_Fiscal = "NOMBRE_FISCAL";
            public const String Campo_Nombre_Comercial = "NOMBRE_COMERCIAL";
            public const String Campo_RFC = "RFC";
            public const String Campo_Cuenta_Contable = "CUENTA_CONTABLE";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Aseguradora_Contacto
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_ASEGURADORA_CONTACTO
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 24/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Aseguradora_Contacto
        {
            public const String Tabla_Cat_Pat_Aseguradora_Contacto = "CAT_PAT_ASEGURADORA_CONTACTO";
            public const String Campo_Aseguradora_Contacto_ID = "ASEGURADORA_CONTACTO_ID";
            public const String Campo_Aseguradora_ID = "ASEGURADORA_ID";
            public const String Campo_Dato_Contacto = "DATO_CONTACTO";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Registrado = "REGISTRADO";
            public const String Campo_Telefono = "TELEFONO";
            public const String Campo_Fax = "FAX";
            public const String Campo_Celular = "CELULAR";
            public const String Campo_Correo_Electronico = "CORREO_ELECTRONICO";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Aseg_Contacto
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_ASEG_DOMICILIO
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 24/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Aseg_Domicilio
        {
            public const String Tabla_Cat_Pat_Aseg_Domicilio = "CAT_PAT_ASEG_DOMICILIO";
            public const String Campo_Aseguradora_Domicilio_ID = "ASEGURADORA_DOMICILIO_ID";
            public const String Campo_Aseguradora_ID = "ASEGURADORA_ID";
            public const String Campo_Domicilio = "DOMICILIO";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Registrado = "REGISTRADO";
            public const String Campo_Calle = "CALLE";
            public const String Campo_Numero_Exterior = "NUMERO_EXTERIOR";
            public const String Campo_Numero_Interior = "NUMERO_INTERIOR";
            public const String Campo_Fax = "FAX";
            public const String Campo_Colonia = "COLONIA";
            public const String Campo_Codigo_Postal = "CODIGO_POSTAL";
            public const String Campo_Ciudad = "CUIDAD";
            public const String Campo_Municipio = "MUNICIPIO";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Pais = "PAIS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Aseguradora_Bancos
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_ASEGURADORA_BANCOS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 24/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Aseguradora_Bancos
        {
            public const String Tabla_Cat_Pat_Aseguradora_Bancos = "CAT_PAT_ASEGURADORA_BANCOS";
            public const String Campo_Aseguradora_Banco_ID = "ASEGURADORA_BANCO_ID";
            public const String Campo_Aseguradora_ID = "ASEGURADORA_ID";
            public const String Campo_Producto_Bancario = "PRODUCTO_BANCARIO";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Registrado = "REGISTRADO";
            public const String Campo_Institucion_Bancaria = "INSTITUCION_BANCARIA";
            public const String Campo_Cuenta = "CUENTA";
            public const String Campo_Clabe_Institucion_Bancaria = "CLABE_INSTITUCION_BANCARIA";
            public const String Campo_Clabe_Plaza = "CLABE_PLAZA";
            public const String Campo_Clabe_Cuenta = "CLABE_CUENTA";
            public const String Campo_Clabe_Digito_Verificador = "CLABE_DIGITO_VERIFICADOR";
            public const String Campo_Clave_Cie = "CLAVE_CIE";
            public const String Campo_Numero_Tarjeta = "NUMERO_TARJETA";
            public const String Campo_Numero_Tarjeta_Reverso = "NUMERO_TARJETA_REVERSO";
            public const String Campo_Fecha_Vigencia = "FECHA_VIGENCIA";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Bienes_Muebles
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_BIENES_MUEBLES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 26/Noviembre/2010 
        /// MODIFICO          : Salvador Hernández Ramírez
        /// FECHA_MODIFICO    : 08/Enero/2011
        /// CAUSA_MODIFICACIÓN: Se agregaron las constantes "Campo_Cantidad, Campo_Fecha_Inventario, Campo_Proveniente,Campo_Donador_ID,Campo_Fecha_Adquisicion,Campo_Nombre,Campo_Marca_ID,Campo_Modelo_ID"
        ///*******************************************************************************
        public class Ope_Pat_Bienes_Muebles
        {
            public const String Tabla_Ope_Pat_Bienes_Muebles = "OPE_PAT_BIENES_MUEBLES";
            public const String Campo_Bien_Mueble_ID = "BIEN_MUEBLE_ID";
            public const String Campo_Producto_ID = "PRODUCTO_ID";
            public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
            public const String Campo_Gerencia_ID = "GERENCIA_ID";
            public const String Campo_Area_ID = "AREA_ID";
            public const String Campo_Material_ID = "MATERIAL_ID";
            public const String Campo_Color_ID = "COLOR_ID";
            public const String Campo_Clasificacion_ID = "CLASIFICACION_ID";
            public const String Campo_Clase_Activo_ID = "CLASE_ACTIVO_ID";
            public const String Campo_Numero_Inventario = "NUMERO_INVENTARIO";
            public const String Campo_Clave_Sistema = "CLAVE_SISTEMA";
            public const String Campo_Clave_Inventario = "CLAVE_INVENTARIO";
            public const String Campo_Factura = "FACTURA";
            public const String Campo_Numero_Serie = "NUMERO_SERIE";
            public const String Campo_Costo_Actual = "COSTO_ACTUAL";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Motivo_Baja = "MOTIVO_BAJA";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Observadores = "OBSERVACIONES";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Cantidad = "CANTIDAD";
            public const String Campo_Fecha_Inventario = "FECHA_INVENTARIO";
            public const String Campo_Procedencia = "PROCEDENCIA";
            public const String Campo_Proveniente = "PROVENIENTE";
            public const String Campo_Donador_ID = "DONADOR_ID";
            public const String Campo_Fecha_Adquisicion = "FECHA_ADQUISICION";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Marca_ID = "MARCA_ID";
            public const String Campo_Modelo_ID = "MODELO_ID";
            public const String Campo_Bien_Parent_ID = "BIEN_PARENT_ID";
            public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
            public const String Campo_Operacion = "OPERACION";
            public const String Campo_Modelo = "MODELO";
            public const String Campo_Garantia = "GARANTIA";
            public const String Campo_No_Inventario_Anterior = "NO_INVENTARIO_ANTERIOR";
            public const String Campo_Zona_ID = "ZONA_ID";
            public const String Campo_Costo_Inicial = "COSTO_INICIAL";
            public const String Campo_Costo_Final = "COSTO_FINAL";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Bienes_Resguardos
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_BIEN_MUE_RESGUARDOS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 29/Noviembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Bienes_Resguardos
        {
            public const String Tabla_Ope_Pat_Bienes_Resguardos = "OPE_PAT_BIENES_RESGUARDOS";
            public const String Campo_Bien_Resguardo_ID = "BIEN_RESGUARDO_ID";
            public const String Campo_Bien_ID = "BIEN_ID";
            public const String Campo_Tipo = "TIPO";
            public const String Campo_Empleado_Resguardo_ID = "EMPLEADO_RESGUARDO_ID";
            public const String Campo_Fecha_Inicial = "FECHA_INICIAL";
            public const String Campo_Fecha_Final = "FECHA_FINAL";
            public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Vehiculos
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_VEHICULOS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 03/Diciembre/2010 
        /// MODIFICO          : Salvador Hernández Ramírez
        /// FECHA_MODIFICO    : 28/diciembre/2010
        /// CAUSA_MODIFICACIÓN: Se agrego la constante "Campo_Cantidad"
        ///*******************************************************************************
        public class Ope_Pat_Vehiculos
        {
            public const String Tabla_Ope_Pat_Vehiculos = "OPE_PAT_VEHICULOS";
            public const String Campo_Vehiculo_ID = "VEHICULO_ID";
            public const String Campo_Producto_ID = "PRODUCTO_ID";
            public const String Campo_Tipo_Vehiculo_ID = "TIPO_VEHICULO_ID";
            public const String Campo_Tipo_Combustible_ID = "TIPO_COMBUSTIBLE_ID";
            public const String Campo_Color_ID = "COLOR_ID";
            public const String Campo_Zona_ID = "ZONA_ID";
            public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
            public const String Campo_Gerencia_ID = "Gerencia_ID";
            public const String Campo_Numero_Inventario = "NUMERO_INVENTARIO";
            public const String Campo_Numero_Economico = "NUMERO_ECONOMICO";
            public const String Campo_Capacidad_Carga = "CAPACIDAD_CARGA";
            public const String Campo_Placas = "PLACAS";
            public const String Campo_Anio_Fabricacion = "ANIO_FABRICACION";
            public const String Campo_Serie_Carroceria = "SERIE_CARROCERIA";
            public const String Campo_Serie_Motor = "SERIE_MOTOR";
            public const String Campo_Numero_Cilindros = "NUMERO_CILINDROS";
            public const String Campo_Kilometraje = "KILOMETRAJE";
            public const String Campo_Numero_Ejes = "NUMERO_EJES";
            public const String Campo_Odometro = "ODOMETRO";
            public const String Campo_Fecha_Adquisicion = "FECHA_ADQUISICION";
            public const String Campo_Costo_Actual = "COSTO_ACTUAL";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Clave_Programatica_Revision = "CLAVE_PROGRAMATICA_REVISION";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Motivo_Baja = "MOTIVO_BAJA";
            public const String Campo_Cantidad = "CANTIDAD";
            public const String Campo_Procedencia = "PROCEDENCIA";
            public const String Campo_Proveniente = "PROVENIENTE";
            public const String Campo_Donador_ID = "DONADOR_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Marca_ID = "MARCA_ID";
            public const String Campo_Modelo_ID = "MODELO_ID";
            public const String Campo_No_Factura = "NO_FACTURA";
            public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
            public const String Campo_Modelo = "MODELO";
            public const String Campo_Empleado_Operador_ID = "EMPLEADO_OPERADOR_ID";
            public const String Campo_Empleado_Recibe_ID = "EMPLEADO_RECIBE_ID";
            public const String Campo_Empleado_Autorizo_ID = "EMPLEADO_AUTORIZO_ID";
            public const String Campo_Clasificacion_ID = "CLASIFICACION_ID";
            public const String Campo_Clase_Activo_ID = "CLASE_ACTIVO_ID";
            public const String Campo_Costo_Inicial = "COSTO_INICIAL";
            public const String Campo_Costo_Final = "COSTO_FINAL";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Vehiculo_Aseguradora
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_VEHICULO_ASEGURADORA
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 08/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Vehiculo_Aseguradora
        {
            public const String Tabla_Ope_Pat_Vehiculo_Aseguradora = "OPE_PAT_VEHICULO_ASEGURADORA";
            public const String Campo_Vehiculo_Aseguradora_ID = "VEHICULO_ASEGURADORA_ID";
            public const String Campo_Aseguradora_ID = "ASEGURADORA_ID";
            public const String Campo_Tipo_Vehiculo_ID = "TIPO_VEHICULO_ID";
            public const String Campo_Descripcion_Seguro = "DESCRIPCION_SEGURO";
            public const String Campo_Cobertura = "COBERTURA";
            public const String Campo_No_Poliza = "NO_POLIZA";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Vehiculo_ID = "VEHICULO_ID";
        }




        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipos_Alimentacion
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPOS_ALIMENTACION
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipos_Alimentacion
        {
            public const String Tabla_Cat_Pat_Tipos_Alimentacion = "CAT_PAT_TIPOS_ALIMENTACION";
            public const String Campo_Tipo_Alimentacion_ID = "TIPO_ALIMENTACION_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Vacunas
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_VACUNAS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Vacunas
        {
            public const String Tabla_Cat_Pat_Vacunas = "CAT_PAT_VACUNAS";
            public const String Campo_Vacuna_ID = "VACUNA_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Fecha_Aplicacion = "FECHA_APLICACION";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipos_Adiestramiento
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPOS_ADIESTRAMIENTO
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipos_Adiestramiento
        {
            public const String Tabla_Cat_Pat_Tipos_Adiestramiento = "CAT_PAT_TIPOS_ADIESTRAMIENTO";
            public const String Campo_Tipo_Adiestramiento_ID = "TIPO_ADIESTRAMIENTO_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Razas
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_RAZAS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Razas
        {
            public const String Tabla_Cat_Pat_Razas = "CAT_PAT_RAZAS";
            public const String Campo_Raza_ID = "RAZA_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipos_Ascendencia
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPOS_ASCENDENCIA
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipos_Ascendencia
        {
            public const String Tabla_Cat_Pat_Tipos_Ascendencia = "CAT_PAT_TIPOS_ASCENDENCIA";
            public const String Campo_Tipo_Ascendencia_ID = "TIPO_ASCENDENCIA_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Funciones
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_FUNCIONES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Funciones
        {
            public const String Tabla_Cat_Pat_Funciones = "CAT_PAT_FUNCIONES";
            public const String Campo_Funcion_ID = "FUNCION_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Veterinarios
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_VETERINARIOS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Veterinarios
        {
            public const String Tabla_Cat_Pat_Veterinarios = "CAT_PAT_VETERINARIOS";
            public const String Campo_Veterinario_ID = "VETERINARIO_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Apellido_Paterno = "APELLIDO_PATERNO";
            public const String Campo_Apellido_Materno = "APELLIDO_MATERNO";
            public const String Campo_Direccion = "DIRECCION";
            public const String Campo_Cuidad = "CUIDAD";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Telefono = "TELEFONO";
            public const String Campo_Celular = "CELULAR";
            public const String Campo_CURP = "CURP";
            public const String Campo_RFC = "RFC";
            public const String Campo_Cedula_Profesional = "CEDULA_PROFESIONAL";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Cemovientes
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_CEMOVIENTES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Diciembre/2010 
        /// MODIFICO          :   Salvador Hernández Ramírez
        /// FECHA_MODIFICO    :  28/Diciembre/2010
        /// CAUSA_MODIFICACIÓN:  Se agrego la constante "Campo_Cantidad"
        ///*******************************************************************************
        public class Ope_Pat_Cemovientes
        {
            public const String Tabla_Ope_Pat_Cemovientes = "OPE_PAT_CEMOVIENTES";
            public const String Campo_Cemoviente_ID = "CEMOVIENTE_ID";
            public const String Campo_Producto_ID = "PRODUCTO_ID";
            public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
            public const String Campo_Tipo_Alimentacion_ID = "TIPO_ALIMENTACION_ID";
            public const String Campo_Tipo_Adiestramiento_ID = "TIPO_ADIESTRAMIENTO_ID";
            public const String Campo_Raza_ID = "RAZA_ID";
            public const String Campo_Funcion_ID = "FUNCION_ID";
            public const String Campo_Veterinario_ID = "VETERINARIO_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Numero_Inventario = "NUMERO_INVENTARIO";
            public const String Campo_Color_ID = "COLOR_ID";
            public const String Campo_Costo_Actual = "COSTO_ACTUAL";
            public const String Campo_Tipo_Ascendencia = "TIPO_ASCENDENCIA";
            public const String Campo_Padre_ID = "PADRE_ID";
            public const String Campo_Madre_ID = "MADRE_ID";
            public const String Campo_Sexo = "SEXO";
            public const String Campo_Tipo_Cemoviente_ID = "TIPO_CEMOVIENTE_ID";
            public const String Campo_Fecha_Nacimiento = "FECHA_NACIMIENTO";
            public const String Campo_Fecha_Adquisicion = "FECHA_ADQUISICION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Motivo_Baja = "MOTIVO_BAJA";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Cantidad = "CANTIDAD";

            public const String Campo_Procedencia = "PROCEDENCIA";
            public const String Campo_Proveniente = "PROVENIENTE";
            public const String Campo_Donador_ID = "DONADOR_ID";

            public const String Campo_No_Factura = "NO_FACTURA";
            public const String Campo_Proveedor_ID = "PROVEEDOR_ID";
            public const String Campo_No_Inventario_Anterior = "NO_INVENTARIO_ANTERIOR";

            public const String Campo_Clasificacion_ID = "CLASIFICACION_ID";
            public const String Campo_Clase_Activo_ID = "CLASE_ACTIVO_ID";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipos_Cemovientes
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPOS_CEMOVIENTES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 21/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipos_Cemovientes
        {
            public const String Tabla_Cat_Pat_Tipos_Cemovientes = "CAT_PAT_TIPOS_CEMOVIENTES";
            public const String Campo_Tipo_Cemoviente_ID = "TIPO_CEMOVIENTE_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }



        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Vacunas_Cemovientes
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_VACUNAS_CEMOVIENTES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 21/Diciembre/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Vacunas_Cemovientes
        {
            public const String Tabla_Ope_Pat_Vacunas_Cemovientes = "OPE_PAT_VACUNAS_CEMOVIENTES";
            public const String Campo_Vacuna_Cemoviente_ID = "VACUNA_CEMOVIENTE_ID";
            public const String Campo_Vacuna_ID = "VACUNA_ID";
            public const String Campo_Cemoviente_ID = "CEMOVIENTE_ID";
            public const String Campo_Veterinario_ID = "VETERINARIO_ID";
            public const String Campo_Fecha_Aplicacion = "FECHA_APLICACION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Motivo_Cancelacion = "MOTIVO_CANCELACION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipos_Siniestros
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPOS_SINIESTROS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 15/Enero/2011 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipos_Siniestros
        {
            public const String Tabla_Cat_Pat_Tipos_Siniestros = "CAT_PAT_TIPOS_SINIESTROS";
            public const String Campo_Tipo_Siniestro_ID = "TIPO_SINIESTRO_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Sinies_Observaciones
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_SINIES_OBSERVACIONES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 15/Enero/2011 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Sinies_Observaciones
        {
            public const String Tabla_Ope_Pat_Sinies_Observaciones = "OPE_PAT_SINIES_OBSERVACIONES";
            public const String Campo_Observacion_ID = "OBSERVACION_ID";
            public const String Campo_Siniestro_ID = "SINIESTRO_ID";
            public const String Campo_Fecha = "FECHA";
            public const String Campo_Observacion = "OBSERVACION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Siniestros
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_SINIESTROS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 15/Enero/2011 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Siniestros
        {
            public const String Tabla_Ope_Pat_Siniestros = "OPE_PAT_SINIESTROS";
            public const String Campo_Siniestro_ID = "SINIESTRO_ID";
            public const String Campo_Tipo_Siniestro_ID = "TIPO_SINIESTRO_ID";
            public const String Campo_Bien_ID = "BIEN_ID";
            public const String Campo_Aseguradora_ID = "ASEGURADORA_ID";
            public const String Campo_Tipo_Bien = "TIPO_BIEN";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Fecha = "FECHA";
            public const String Campo_Parte_Averiguacion = "PARTE_AVERIGUACION";
            public const String Campo_Reparacion = "REPARACION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Responsable_Municipio = "RESPONSABLE_MUNICIPIO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Numero_Reporte = "NUMERO_REPORTE";
            public const String Campo_Consignado = "CONSIGNADO";
            public const String Campo_Pago_Danio_Sindicos = "PAGO_DANIO_SINDICOS";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Donadores
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_DONADORES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 21/enero/2010 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Donadores
        {
            public const String Tabla_Cat_Pat_Donadores = "CAT_PAT_DONADORES";
            public const String Campo_Donador_ID = "DONADOR_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Apellido_Paterno = "APELLIDO_PATERNO";
            public const String Campo_Apellido_Materno = "APELLIDO_MATERNO";
            public const String Campo_Direccion = "DIRECCION";
            public const String Campo_Cuidad = "CUIDAD";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Telefono = "TELEFONO";
            public const String Campo_Celular = "CELULAR";
            public const String Campo_CURP = "CURP";
            public const String Campo_RFC = "RFC";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Bienes_Caja_Chica
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_BIENES_CAJA_CHICA
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 24/Enero/2010 
        /// MODIFICO          : 
        /// FECHA_MODIFICO    : 
        /// CAUSA_MODIFICACIÓN: 
        ///*******************************************************************************
        public class Ope_Pat_Bienes_Caja_Chica
        {
            public const String Tabla_Ope_Pat_Bienes_Caja_Chica = "OPE_PAT_BIENES_CAJA_CHICA";
            public const String Campo_Bien_ID = "BIEN_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Marca_ID = "MARCA_ID";
            public const String Campo_Modelo_ID = "MODELO_ID";
            public const String Campo_Costo = "COSTO";
            public const String Campo_Cantidad = "CANTIDAD";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Dependencia_ID = "DEPENDENCIA_ID";
            public const String Campo_Numero_Inventario = "NUMERO_INVENTARIO";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Material_ID = "MATERIAL_ID";
            public const String Campo_Color_ID = "COLOR_ID";
            public const String Campo_Motivo_Baja = "MOTIVO_BAJA";
            public const String Campo_Fecha_Adquisicion = "FECHA_ADQUISICION";
        }



        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Tabla_Ope_Pat_Partes_Vehiculos
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_PARTES_VEHICULOS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 02/Febrero/2011 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///********************************************************************************
        public class Ope_Pat_Partes_Vehiculos
        {
            public const String Tabla_Ope_Pat_Partes_Vehiculos = "OPE_PAT_PARTES_VEHICULOS";
            public const String Campo_Parte_ID = "PARTE_ID";
            public const String Campo_Producto_ID = "PRODUCTO_ID";
            public const String Campo_Vehiculo_ID = "VEHICULO_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Marca_ID = "MARCA_ID";
            public const String Campo_Modelo_ID = "MODELO_ID";
            public const String Campo_Costo = "COSTO";
            public const String Campo_Material_ID = "MATERIAL_ID";
            public const String Campo_Color_ID = "COLOR_ID";
            public const String Campo_Cantidad = "CANTIDAD";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Numero_Inventario = "NUMERO_INVENTARIO";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Motivo_Baja = "MOTIVO_BAJA";
            public const String Campo_Fecha_Adquisicion = "FECHA_ADQUISICION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Modelo = "MODELO";
        }



        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Archivos_Bienes
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_ARCHIVOS_BIENES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 14/Febrero/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Archivos_Bienes
        {
            public const String Tabla_Ope_Pat_Archivos_Bienes = "OPE_PAT_ARCHIVOS_BIENES";
            public const String Campo_Archivo_Bien_ID = "ARCHIVO_BIEN_ID";
            public const String Campo_Bien_ID = "BIEN_ID";
            public const String Campo_Tipo = "TIPO";
            public const String Campo_Fecha = "FECHA";
            public const String Campo_Archivo = "ARCHIVO";
            public const String Campo_Tipo_Archivo = "TIPO_ARCHIVO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Ruta_Fisica_Archivos = "CONTROL_PATRIMONIAL_ARCHIVOS_BIENES";
            public const String Campo_Descripcion_Archivo = "DESCRIPCION_ARCHIVO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipo_Vehiculo_Archivo
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPO_VEHICULO_ARCHIVO
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 01/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipo_Vehiculo_Archivo
        {
            public const String Tabla_Cat_Pat_Tipo_Vehiculo_Archivo = "CAT_PAT_TIPO_VEHICULO_ARCHIVO";
            public const String Campo_Archivo_ID = "ARCHIVO_ID";
            public const String Campo_Tipo_Vehiculo_ID = "TIPO_VEHICULO_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Fecha = "FECHA";
            public const String Campo_Nombre_Archivo = "NOMBRE_ARCHIVO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Ruta_Fisica_Archivos = "CONTROL_PATRIMONIAL_ARCHIVOS_TIPOS_VEHICULOS";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Det_Vehiculos
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_DET_VEHICULOS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 07/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Det_Vehiculos
        {
            public const String Tabla_Cat_Pat_Det_Vehiculos = "CAT_PAT_DET_VEHICULOS";
            public const String Campo_Detalle_Vehiculo_ID = "DETALLE_VEHICULO_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Fecha = "FECHA";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Tipo_Veh_Detalles
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPO_VEH_DETALLES
        /// PARÁMETROS :      
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 07/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Tipo_Veh_Detalles
        {
            public const String Tabla_Cat_Pat_Tipo_Veh_Detalles = "CAT_PAT_TIPO_VEH_DETALLES";
            public const String Campo_Registro_ID = "REGISTRO_ID";
            public const String Campo_Detalle_Vehiculo_ID = "DETALLE_VEHICULO_ID";
            public const String Campo_Tipo_Vehiculo_ID = "TIPO_VEHICULO_ID";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Procedencias
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_PROCEDENCIAS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 07/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Procedencias
        {
            public const String Tabla_Cat_Pat_Procedencias = "CAT_PAT_PROCEDENCIAS";
            public const String Campo_Procedencia_ID = "PROCEDENCIA_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Vehiculo_Detalles
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_TIPO_VEH_DETALLES
        /// PARÁMETROS :      
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 09/Julio/2011
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Vehiculo_Detalles
        {
            public const String Tabla_Cat_Pat_Vehiculo_Detalles = "OPE_PAT_VEHICULO_DETALLES";
            public const String Campo_Registro_ID = "REGISTRO_ID";
            public const String Campo_Vehiculo_ID = "VEHICULO_ID";
            public const String Campo_Detalle_Vehiculo_ID = "DETALLE_VEHICULO_ID";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Usos_Inmuebles
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_USOS_INMUEBLES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Febrero/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Usos_Inmuebles
        {
            public const String Tabla_Cat_Pat_Usos_Inmuebles = "CAT_PAT_USOS_INMUEBLES";
            public const String Campo_Uso_ID = "USO_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Origenes_Inmuebles
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_ORIGENES_INMUEBLES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 06/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Origenes_Inmuebles
        {
            public const String Tabla_Cat_Pat_Origenes_Inmuebles = "CAT_PAT_ORIGENES_INMUEBLES";
            public const String Campo_Origen_ID = "ORIGEN_ID";
            public const String Campo_Nombre = "NOMBRE";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Destinos_Inmuebles
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_DESTINOS_INMUEBLES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 23/Febrero/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Destinos_Inmuebles
        {
            public const String Tabla_Cat_Pat_Destinos_Inmuebles = "CAT_PAT_DESTINOS_INMUEBLES";
            public const String Campo_Destino_ID = "DESTINO_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Bienes_Inmuebles
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_BIENES_INMUEBLES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 28/Febrero/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Bienes_Inmuebles
        {
            public const String Tabla_Ope_Pat_Bienes_Inmuebles = "OPE_PAT_BIENES_INMUEBLES";
            public const String Campo_Bien_Inmueble_ID = "BIEN_INMUEBLE_ID";
            public const String Campo_Calle_ID = "CALLE_ID";
            public const String Campo_Colonia_ID = "COLONIA_ID";
            public const String Campo_Uso_ID = "USO_ID";
            public const String Campo_Superficie = "SUPERFICIE";
            public const String Campo_Construccion = "CONSTRUCCION";
            public const String Campo_Manzana = "MANZANA";
            public const String Campo_Cuenta_Predial_ID = "CUENTA_PREDIAL_ID";
            public const String Campo_Lote = "LOTE";
            public const String Campo_Porcentaje_Ocupacion = "PORCENTAJE_OCUPACION";
            public const String Campo_Efectos_Fiscales = "EFECTOS_FISCALES";
            public const String Campo_Clasificacion_Zona_ID = "CLASIFICACION_ZONA";
            public const String Campo_Tipo_Predio_ID = "TIPO_PREDIO_ID";
            public const String Campo_Vias_Acceso = "VIAS_ACCESO";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Densidad_Construccion = "DENSIDAD_CONTRUCCION";
            public const String Campo_Valor_Comercial = "VALOR_COMERCIAL";
            public const String Campo_Numero_Exterior = "NUMERO_EXTERIOR";
            public const String Campo_Numero_Interior = "NUMERO_INTERIOR";
            public const String Campo_Destino_ID = "DESTINO_ID";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Origen_ID = "ORIGEN_ID";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Fecha_Registro = "FECHA_REGISTRO";
            public const String Campo_Sector_ID = "SECTOR_ID";
            public const String Campo_Area_ID = "AREA_ID";

            public const String Campo_Hoja = "HOJA";
            public const String Campo_Tomo = "TOMO";
            public const String Campo_Numero_Acta = "NUMERO_ACTA";
            public const String Campo_Cartilla_Parcelaria = "CARTILLA_PARCELARIA";
            public const String Campo_Superficie_Contable = "SUPERFICIE_CONTABLE";
            public const String Campo_Unidad_Superficie = "UNIDAD_SUPERFICIE";
            public const String Campo_Clase_Activo_ID = "CLASE_ACTIVO_ID";
            public const String Campo_Tipo_Bien = "TIPO_BIEN";
            public const String Campo_Fecha_Baja = "FECHA_BAJA";
            public const String Campo_Registro_Propiedad = "REGISTRO_PROPIEDAD";
            public const String Campo_Fecha_Alta_Cta_Pub = "FECHA_ALTA_CTA_PUB";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_B_Inm_Observaciones
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_B_INM_OBSERVACIONES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 1/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_B_Inm_Observaciones
        {
            public const String Tabla_Ope_Pat_B_Inm_Observaciones = "OPE_PAT_B_INM_OBSERVACIONES";
            public const String Campo_No_Observacion = "NO_OBSERVACION";
            public const String Campo_Bien_Inmueble_ID = "BIEN_INMUEBLE_ID";
            public const String Campo_Fecha_Observacion = "FECHA_OBSERVACION";
            public const String Campo_Usuario_Observacion_ID = "USUARIO_OBSERVACION_ID";
            public const String Campo_Observacion = "OBSERVACION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_B_Inm_Medidas
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_B_INM_MEDIDAS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 1/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_B_Inm_Medidas
        {
            public const String Tabla_Ope_Pat_B_Inm_Medidas = "OPE_PAT_B_INM_MEDIDAS";
            public const String Campo_No_Registro = "NO_REGISTRO";
            public const String Campo_Bien_Inmueble_ID = "BIEN_INMUEBLE_ID";
            public const String Campo_Segun = "SEGUN";
            public const String Campo_Orientacion_ID = "ORIENTACION_ID";
            public const String Campo_Medida = "MEDIDA";
            public const String Campo_Colindancia = "COLINDANCIA";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_B_Inm_Archivos
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_B_INM_ARCHIVOS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 1/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_B_Inm_Archivos
        {
            public const String Tabla_Ope_Pat_B_Inm_Archivos = "OPE_PAT_B_INM_ARCHIVOS";
            public const String Campo_No_Registro = "NO_REGISTRO";
            public const String Campo_Bien_Inmueble_ID = "BIEN_INMUEBLE_ID";
            public const String Campo_Tipo_Archivo = "TIPO_ARCHIVO";
            public const String Campo_Descripcion_Archivo = "DESCRIPCION_ARCHIVO";
            public const String Campo_Ruta_Archivo = "RUTA_ARCHIVO";
            public const String Campo_Usuario_Cargo_ID = "USUARIO_CARGO_ID";
            public const String Campo_Fecha_Cargo = "FECHA_CARGO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Estatus = "ESTATUS";
            public const String Ruta_Archivos_Inmuebles = "../../CONTROL_PATRIMONIAL_ARCHIVOS_BIENES/BIENES_INMUEBLES/";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_B_Inm_Juridico
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_B_INM_ARCHIVOS
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 1/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_B_Inm_Juridico
        {
            public const String Tabla_Ope_Pat_B_Inm_Juridico = "OPE_PAT_B_INM_JURIDICO";
            public const String Campo_No_Registro = "NO_REGISTRO";
            public const String Campo_Bien_Inmueble_ID = "BIEN_INMUEBLE_ID";
            public const String Campo_Escritura = "ESCRITURA";
            public const String Campo_Fecha_Escritura = "FECHA_ESCRITURA";
            public const String Campo_No_Notario = "NO_NOTARIO";
            public const String Campo_Nombre_Notario = "NOMBRE_NOTARIO";
            public const String Campo_Constancia_Registral = "CONSTANCIA_REGISTRAL";
            public const String Campo_Folio_Real = "FOLIO_REAL";
            public const String Campo_Libertad_Gravament = "LIBERTAD_GRAVAMEN";
            public const String Campo_Antecedente_Registral = "ANTECEDENTE_REGISTRAL";
            public const String Campo_Proveedor = "PROVEEDOR";
            public const String Campo_Observaciones = "OBSERVACIONES";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_No_Contrato = "NO_CONTRATO";
            public const String Campo_Movimiento = "MOVIMIENTO";
            public const String Campo_Nuevo_Propietario = "NUEVO_PROPIETARIO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Sessiones
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_SESSIONES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 1/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Sessiones
        {
            public const String Tabla_Ope_Pat_Sessiones = "OPE_PAT_SESSIONES";
            public const String Campo_No_Registro = "NO_REGISTRO";
            public const String Campo_Folio = "FOLIO";
            public const String Campo_Fecha_Session = "FECHA_SESSION";
            public const String Campo_Ruta_Archivo = "RUTA_ARCHIVO";
            public const String Campo_Usuario_Creo_ID = "USUARIO_CREO_ID";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico_ID = "USUARIO_MODIFICO_ID";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Orientaciones_Inm
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_ORIENTACIONES_INM
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 09/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Orientaciones_Inm
        {
            public const String Tabla_Cat_Pat_Orientaciones_Inm = "CAT_PAT_ORIENTACIONES_INM";
            public const String Campo_Orientacion_ID = "ORIENTACION_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Clasif_Zona_Inm
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_CLASIF_ZONA_INM
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 10/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Clasif_Zona_Inm
        {
            public const String Tabla_Cat_Pat_Clasif_Zona_Inm = "CAT_PAT_CLASIF_ZONA_INM";
            public const String Campo_Clasificacion_ID = "CLASIFICACION_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_B_Inm_Expropiaciones
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_B_INM_EXPROPIACIONES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 15/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_B_Inm_Expropiaciones
        {
            public const String Tabla_Ope_Pat_B_Inm_Expropiaciones = "OPE_PAT_B_INM_EXPROPIACIONES";
            public const String Campo_No_Expropiacion = "NO_EXPROPIACION";
            public const String Campo_Bien_Inmueble_ID = "BIEN_INMUEBLE_ID";
            public const String Campo_Fecha_Expropiacion = "FECHA_EXPROPIACION";
            public const String Campo_Usuario_Expropiacion_ID = "USUARIO_EXPROPIACION_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_B_Inm_Afectaciones
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla OPE_PAT_B_INM_AFECTACIONES
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 15/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_B_Inm_Afectaciones
        {
            public const String Tabla_Ope_Pat_B_Inm_Afectaciones = "OPE_PAT_B_INM_AFECTACIONES";
            public const String Campo_No_Registro = "NO_REGISTRO";
            public const String Campo_Bien_Inmueble_ID = "BIEN_INMUEBLE_ID";
            public const String Campo_Fecha_Afectacion = "FECHA_AFECTACION";
            public const String Campo_Fecha_Registro = "FECHA_REGISTRO";
            public const String Campo_Usuario_Registro_ID = "USUARIO_REGISTRO_ID";
            public const String Campo_Propietario = "PROPIETARIO";
            public const String Campo_Session_Ayuntamiento = "SESSION_AYUNTAMIENTO";
            public const String Campo_Tramo = "TRAMO";
            public const String Campo_No_Contrato = "NO_CONTRATO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";

        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Pat_Areas_Donacion
        /// DESCRIPCIÓN: Clase que contiene los campos de la tabla CAT_PAT_AREAS_DONACION
        /// PARÁMETROS :     
        /// CREO       : Franciso Antonio Gallardo Castañeda
        /// FECHA_CREO : 15/Marzo/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Pat_Areas_Donacion
        {
            public const String Tabla_Cat_Pat_Areas_Donacion = "CAT_PAT_AREAS_DONACION";
            public const String Campo_Area_ID = "AREA_ID";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }


        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Bienes_Custodias
        /// DESCRIPCIÓN:        Clase que contiene los campos de la tabla OPE_PAT_BIENES_CUSTODIAS
        /// PARÁMETROS :     
        /// CREO       :        Luis Daniel Guzmán Malagón
        /// FECHA_CREO :        22/Septiembre/2012
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Bienes_Custodias
        {
            public const String Tabla_Ope_Pat_Bienes_Custodias = "OPE_PAT_BIENES_CUSTODIAS";
            public const String Campo_Bien_Custodia_ID = "BIEN_CUSTODIA_ID";
            public const String Campo_Bien_ID = "BIEN_ID";
            public const String Campo_Tipo = "TIPO";
            public const String Campo_Empleado_Recibo_ID = "EMPLEADO_RECIBO_ID";
            public const String Campo_Fecha_Inicial = "FECHA_INICIAL";
            public const String Campo_Fecha_Final = "FECHA_FINAL";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
        }

        /////*******************************************************************************
        ///// NOMBRE DE LA CLASE: Ope_Pat_Bienes_Custodias
        ///// DESCRIPCIÓN:        Clase que contiene los campos de la tabla OPE_PAT_BIENES_CUSTODIAS
        ///// PARÁMETROS :     
        ///// CREO       :        Luis Daniel Guzmán Malagón
        ///// FECHA_CREO :        22/Septiembre/2012
        ///// MODIFICO          :
        ///// FECHA_MODIFICO    :
        ///// CAUSA_MODIFICACIÓN:
        /////*******************************************************************************
        //public class Ope_Pat_Bienes_Custodias
        //{
        //    public const String Tabla_Ope_Pat_Bienes_Custodias = "OPE_PAT_BIENES_CUSTODIAS";
        //    public const String Campo_Bien_Custodia_ID = "BIEN_CUSTODIA_ID";
        //    public const String Campo_Bien_ID = "BIEN_ID";
        //    public const String Campo_Tipo = "TIPO";
        //    public const String Campo_Empleado_Recibo_ID = "EMPLEADO_RECIBO_ID";
        //    public const String Campo_Fecha_Inicial = "FECHA_INICIAL";
        //    public const String Campo_Fecha_Final = "FECHA_FINAL";
        //    public const String Campo_Estatus = "ESTATUS";
        //    public const String Campo_Comentarios = "COMENTARIOS";
        //    public const String Campo_Usuario_Creo = "USUARIO_CREO";
        //    public const String Campo_Fecha_Creo = "FECHA_CREO";
        //    public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
        //    public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        //    public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
        //}

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Pat_Bienes_Recibos
        /// DESCRIPCIÓN:        Clase que contiene los campos de la tabla OPE_PAT_BIENES_RECIBOS
        /// PARÁMETROS :     
        /// CREO       :        Salvador Hernandez Ramirez
        /// FECHA_CREO :        26/Julio/20101
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Pat_Bienes_Recibos
        {
            public const String Tabla_Ope_Pat_Bienes_Recibos = "OPE_PAT_BIENES_RECIBOS";
            public const String Campo_Bien_Recibo_ID = "BIEN_RECIBO_ID";
            public const String Campo_Bien_ID = "BIEN_ID";
            public const String Campo_Tipo = "TIPO";
            public const String Campo_Empleado_Recibo_ID = "EMPLEADO_RECIBO_ID";
            public const String Campo_Fecha_Inicial = "FECHA_INICIAL";
            public const String Campo_Fecha_Final = "FECHA_FINAL";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Empleado_Almacen_ID = "EMPLEADO_ALMACEN_ID";
        }

        #endregion Control Patrimonial


        ///*************************************************************************************************************************
        ///                                                    FACTURACIÓN GLOBAL
        ///*************************************************************************************************************************

        #region Facturacion_Global

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Cat_Facturas_Series
        /// DESCRIPCIÓN: 
        /// PARÁMETROS :     
        /// CREO       : Miguel Angel Alvarado Enriquez
        /// FECHA_CREO : 17 Diciembre 2013 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Cat_Facturas_Series {
            public const String Tabla_Cat_Facturas_Series = "CAT_FACTURAS_SERIES";
            public const String Campo_Serie_Id = "SERIE_ID";
            public const String Campo_Serie = "SERIE";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Tipo_Serie = "TIPO_SERIE";
            public const String Campo_Total_Timbres = "TOTAL_TIMBRES";
            public const String Campo_Timbre_Inicial = "TIMBRE_INICIAL";
            public const String Campo_Timbre_Final = "TIMBRE_FINAL";
            public const String Campo_Cancelaciones = "CANCELACIONES";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Tipo_Default = "TIPO_DEFAULT";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Ip_Creo = "IP_CREO";
            public const String Campo_Equipo_Creo = "EQUIPO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Ip_Modifico = "IP_MODIFICO";
            public const String Campo_Equipo_Modifico = "EQUIPO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Apl_Parametros_Correos
        /// DESCRIPCIÓN       : 
        /// PARÁMETROS        :     
        /// CREO              : Miguel Angel Alvarado Enriquez
        /// FECHA_CREO        : 17 Diciembre 2013 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Apl_Parametros_Correos {
            public const String Tabla_Apl_Parametros_Correo = "APL_PARAMETROS_CORREO";
            public const String Campo_Parametro_Id = "PARAMETRO_CORREO_ID";
            public const String Campo_Email = "EMAIL";
            public const String Campo_Contrasenia = "CONTRASENIA";
            public const String Campo_Host = "HOST_EMAIL";
            public const String Campo_Puerto = "PUERTO";
            public const String Campo_Imagen_Empresa = "IMAGEN_EMPRESA";
            public const String Campo_Mensaje_Sistema = "MENSAJE_SISTEMA";
            public const String Campo_Tipo_Movimiento = "TIPO_MOVIMIENTO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Ip_Creo = "IP_CREO";
            public const String Campo_Equipo_Creo = "EQUIPO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Ip_Modifico = "IP_MODIFICO";
            public const String Campo_Equipo_Modifico = "EQUIPO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Apl_Cat_Parametros_Impuestos
        /// DESCRIPCIÓN       : 
        /// PARÁMETROS        :     
        /// CREO              : Miguel Angel Alvarado Enriquez
        /// FECHA_CREO        : 17 Diciembre 2013 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************Apl_Parametros_Factura
        public class Apl_Parametros_Facturacion {
            public const String Tabla_Apl_Cat_Parametros_Facturacion = "APL_PARAMETROS_FACTURA";
            public const String Campo_Parametro_Factura_Id = "PARAMETRO_FACTURACION_ID";
            public const String Campo_Interes_Pagare = "INTERES_PAGARE";
            public const String Campo_IVA = "IVA";
            public const String Campo_Retencion_Fletes = "RETENCION_FLETES";
            public const String Campo_Retencion_IVA = "RETENCION_IVA";
            public const String Campo_Retencion_Cedular = "RETENCION_CEDULAR";
            public const String Campo_Retencion_ISR = "RETENCION_ISR";
            public const String Campo_Email_Origen_Correos = "EMAIL_ORIGEN_CORREOS";
            public const String Campo_Mensaje_Factura = "MENSAJE_FACTURA";
            public const String Campo_Regimen_Fiscal = "REGIMEN_FISCAL";
            public const String Campo_Version = "VERSION";
            public const String Campo_Codigo_Usuario = "CODIGO_USUARIO";
            public const String Campo_Codigo_Usuario_Proveedor = "CODIGO_USUARIO_PROVEEDOR";
            public const String Campo_Sucursal_Id = "SUCURSAL_ID";
            public const String Campo_Ambiente_Timbrado = "AMBIENTE_TIMBRADO";
            public const String Campo_Ruta_Certificado = "RUTA_CERTIFICADO";
            public const String Campo_Ruta_Llave = "RUTA_LLAVE";
            public const String Campo_Ruta_Pdf = "RUTA_PDF";
            public const String Campo_Ruta_Xml = "RUTA_XML";
            public const String Campo_Aviso_Expira_Vigencia = "AVISO_EXPIRA_VIGENCIA";
            public const String Campo_Aviso_Expira_Folio = "AVISO_EXPIRA_FOLIOS";
            public const String Campo_Vigencia_Certificado_Inicio = "VIGENCIA_CERTIFICADO_INICIO";
            public const String Campo_Vigencia_Certificado_Fin = "VIGENCIA_CERTIFICADO_FIN";
            public const String Campo_Password_Llave = "PASSWORD_LLAVE";
            public const String Campo_Imagen_Factura = "IMAGEN_FACTURA";
            public const String Campo_Imagen_Factura_2 = "IMAGEN_FACTURA_2";
            public const String Campo_Imagen_Factura_3 = "IMAGEN_FACTURA_3";
            public const String Campo_Imagen_Factura_4 = "IMAGEN_FACTURA_4";
            public const String Campo_Envio_Automatico_Correo = "ENVIO_AUTOMATICO_CORREO";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Ip_Creo = "IP_CREO";
            public const String Campo_Equipo_Creo = "EQUIPO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Ip_Modifico = "IP_MODIFICO";
            public const String Campo_Equipo_Modifico = "EQUIPO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const string Campo_Permitir_Facturas_Anteriores = "PERMITIR_FACTURAS_ANTERIORES";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Facturas
        /// DESCRIPCIÓN: 
        /// PARÁMETROS :     
        /// CREO       : Miguel Angel Alvarado Enriquez
        /// FECHA_CREO : 17 Diciembre 2013 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Facturas {
            public const String Tabla_Ope_Facturas = "OPE_FACTURAS";
            public const String Campo_No_Factura = "NO_FACTURA";
            public const String Campo_Serie = "SERIE";
            public const String Campo_Cliente_Id = "CLIENTE_ID";
            public const String Campo_Folio_Pago = "FOLIO_PAGO";
            public const String Campo_Email = "EMAIL";
            public const String Campo_Razon_Social = "RAZON_SOCIAL";
            public const String Campo_Rfc = "RFC";
            public const String Campo_Pais = "PAIS";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Localidad = "LOCALIDAD";
            public const String Campo_Colonia = "COLONIA";
            public const String Campo_Ciudad = "CIUDAD";
            public const String Campo_Calle = "CALLE";
            public const String Campo_Numero_Exterior = "NUMERO_EXTERIOR";
            public const String Campo_Numero_Interior = "NUMERO_INTERIOR";
            public const String Campo_Cp = "CP";
            public const String Campo_Dias_Credito = "DIAS_CREDITO";
            public const String Campo_Porcentaje_Descuento = "PORCENTAJE_DESCUENTO";
            public const String Campo_Orden_Compra = "ORDEN_COMPRA";
            public const String Campo_Forma_Pago = "FORMA_PAGO";
            public const String Campo_Condiciones = "CONDICIONES";
            public const String Campo_No_Cuenta_Pago = "NO_CUENTA_PAGO";
            public const String Campo_Metodo_Pago = "METODO_PAGO";
            public const String Campo_Banco_Pago = "BANCO_PAGO";
            public const String Campo_Fecha_Emision = "FECHA_EMISION";
            public const String Campo_Fecha_Vencimiento = "FECHA_VENCIMIENTO";
            public const String Campo_Fecha_Pago = "FECHA_PAGO";
            public const String Campo_Tipo_Factura = "TIPO_FACTURA";
            public const String Campo_Tipo_Moneda = "TIPO_MONEDA";
            public const String Campo_Tipo_Cambio = "TIPO_CAMBIO";
            public const String Campo_Subtotal = "SUBTOTAL";
            public const String Campo_Subtotal_Cero = "SUBTOTAL_CERO";
            public const String Campo_Retencion_Cedular = "RETENCION_CEDULAR";
            public const String Campo_Retencion_Isr = "RETENCION_ISR";
            public const String Campo_Retencion_Iva = "RETENCION_IVA";
            public const String Campo_Retencion_Flete = "RETENCION_FLETE";
            public const String Campo_Iva = "IVA";
            public const String Campo_Descuento = "DESCUENTO";
            public const String Campo_Total = "TOTAL";
            public const String Campo_Saldo = "SALDO";
            public const String Campo_Abono = "ABONO";
            public const String Campo_Pagada = "PAGADA";
            public const String Campo_Cancelada = "CANCELADA";
            public const String Campo_Usuario_Cancelacion = "USUARIO_CANCELACION";
            public const String Campo_Fecha_Cancelacion = "FECHA_CANCELACION";
            public const String Campo_Motivo_Cancelacion = "MOTIVO_CANCELACION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Certificado = "CERTIFICADO";
            public const String Campo_No_Certificado = "NO_CERTIFICADO";
            public const String Campo_No_Autorizacion = "NO_AUTORIZACION";
            public const String Campo_Anio_Autorizacion = "ANIO_AUTORIZACION";
            public const String Campo_Genero_Informe = "GENERO_INFORME";
            public const String Campo_Fecha_Informe_Sat = "FECHA_INFORME_SAT";
            public const String Campo_Usuario_Informe_Sat = "USUARIO_INFORME_SAT";
            public const String Campo_Fecha_Creo_Xml = "FECHA_CREO_XML";
            public const String Campo_Usuario_Autorizo_Regenerar = "USUARIO_AUTORIZO_REGENERAR";
            public const String Campo_Fecha_Autorizo_Regenerar = "FECHA_AUTORIZO_REGENERAR";
            public const String Campo_Usuario_Autorizo_Informe = "USUARIO_AUTORIZO_INFORME";
            public const String Campo_Fecha_Autorizo_Informe = "FECHA_AUTORIZO_INFORME";
            public const String Campo_Timbre_Version = "TIMBRE_VERSION";
            public const String Campo_Timbre_Uuid = "TIMBRE_UUID";
            public const String Campo_Timbre_Fecha_Timbrado = "TIMBRE_FECHA_TIMBRADO";
            public const String Campo_Timbre_Sello_Cfd = "TIMBRE_SELLO_CFD";
            public const String Campo_Timbre_No_Certificado_Sat = "TIMBRE_NO_CERTIFICADO_SAT";
            public const String Campo_Timbre_Sello_Sat = "TIMBRE_SELLO_SAT";
            public const String Campo_Ruta_Codigo_Bd = "RUTA_CODIGO_BD";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Ip_Creo = "IP_CREO";
            public const String Campo_Equipo_Creo = "EQUIPO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Ip_Modifico = "IP_MODIFICO";
            public const String Campo_Equipo_Modifico = "EQUIPO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
            public const String Campo_Curp = "CURP";
            public const String Campo_Fax = "FAX";
            public const String Campo_Telefono_1 = "TELEFONO_1";
            public const String Campo_Telefono_2 = "TELEFONO_2";
            public const String Campo_Extension = "EXTENSION";
            public const String Campo_Nextel = "NEXTEL";
            public const String Campo_Sitio_Web = "SITIO_WEB";
        }
        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Detalles_Facturas
        /// DESCRIPCIÓN: 
        /// PARÁMETROS :     
        /// CREO       : Miguel Angel Alvarado Enriquez
        /// FECHA_CREO : 17 Diciembre 2013 
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Facturas_Detalle {
            public const String Tabla_Ope_Facturas_Detalle = "OPE_FACTURAS_DETALLES";
            public const String Campo_No_Factura = "NO_FACTURA";
            public const String Campo_Producto_Id = "PRODUCTO_ID";
            public const String Campo_Serie = "SERIE";
            public const String Campo_Folio_Pago = "FOLIO_PAGO";
            public const String Campo_Cantidad = "CANTIDAD";
            public const String Campo_Codigo = "CODIGO";
            public const String Campo_Cuenta_Predial = "CUENTA_PREDIAL";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Unidad = "UNIDAD";
            public const String Campo_Porcentaje_Impuesto = "PORCENTAJE_IMPUESTO";
            public const String Campo_Subtotal = "SUBTOTAL";
            public const String Campo_Precio_Unitario = "PRECIO_UNITARIO";
            public const String Campo_Iva = "IVA";
            public const String Campo_Total = "TOTAL";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Ip_Creo = "IP_CREO";
            public const String Campo_Equipo_Creo = "EQUIPO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Ip_Modifico = "IP_MODIFICO";
            public const String Campo_Equipo_Modifico = "EQUIPO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        #endregion
        #region Notas_Credito

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Notas_Credito
        /// DESCRIPCIÓN: 
        /// PARÁMETROS :     
        /// CREO       : Miguel Angel AlvRAdo Enriquez
        /// FECHA_CREO : 24/Febrero/2014
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Notas_Credito {
            public const String Tabla_Ope_Notas_Credito = "OPE_NOTAS_CREDITO";
            public const String Campo_No_Nota_Credito = "NO_NOTA_CREDITO";
            public const String Campo_Serie = "SERIE";
            public const String Campo_No_Factura = "NO_FACTURA";
            public const String Campo_Serie_Factura = "SERIE_FACTURA";
            public const String Campo_Cliente_Id = "CLIENTE_ID";
            public const String Campo_Razon_Social = "RAZON_SOCIAL";
            public const String Campo_Rfc = "RFC";
            public const String Campo_Email = "EMAIL";
            public const String Campo_Pais = "PAIS";
            public const String Campo_Estado = "ESTADO";
            public const String Campo_Localidad = "LOCALIDAD";
            public const String Campo_Colonia = "COLONIA";
            public const String Campo_Ciudad = "CIUDAD";
            public const String Campo_Calle = "CALLE";
            public const String Campo_Numero_Exterior = "NUMERO_EXTERIOR";
            public const String Campo_Numero_Interior = "NUMERO_INTERIOR";
            public const String Campo_Cp = "CP";
            public const String Campo_Metodo_Pago = "METODO_PAGO";
            public const String Campo_Banco_Pago = "BANCO_PAGO";
            public const String Campo_Fecha_Pago = "FECHA_PAGO";
            public const String Campo_Fecha_Emision = "FECHA_EMISION";
            public const String Campo_Tipo_Nota_Credito = "TIPO_NOTA_CREDITO";
            public const String Campo_Tipo_Moneda = "TIPO_MONEDA";
            public const String Campo_Tipo_Cambio = "TIPO_CAMBIO";
            public const String Campo_Subtotal = "SUBTOTAL";
            public const String Campo_Iva = "IVA";
            public const String Campo_Total = "TOTAL";
            public const String Campo_Cancelada = "CANCELADA";
            public const String Campo_Estatus = "ESTATUS";
            public const String Campo_Usuario_Cancelacion = "USUARIO_CANCELACION";
            public const String Campo_Fecha_Cancelacion = "FECHA_CANCELACION";
            public const String Campo_Motivo_Cancelacion = "MOTIVO_CANCELACION";
            public const String Campo_Comentarios = "COMENTARIOS";
            public const String Campo_Certificado = "CERTIFICADO";
            public const String Campo_No_Certificado = "NO_CERTIFICADO";
            public const String Campo_Fecha_Creo_Xml = "FECHA_CREO_XML";
            public const String Campo_Timbre_Version = "TIMBRE_VERSION";
            public const String Campo_Timbre_Uuid = "TIMBRE_UUID";
            public const String Campo_Timbre_Fecha_Timbrado = "TIMBRE_FECHA_TIMBRADO";
            public const String Campo_Timbre_Sello_Cfd = "TIMBRE_SELLO_CFD";
            public const String Campo_Timbre_No_Certificado_Sat = "TIMBRE_NO_CERTIFICADO_SAT";
            public const String Campo_Timbre_Sello_Sat = "TIMBRE_SELLO_SAT";
            public const String Campo_Ruta_Codigo_Bd = "RUTA_CODIGO_BD";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Ip_Creo = "IP_CREO";
            public const String Campo_Equipo_Creo = "EQUIPO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Ip_Modifico = "IP_MODIFICO";
            public const String Campo_Equipo_Modifico = "EQUIPO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        #endregion

        #region Notas_Credito_Detalle

        ///*******************************************************************************
        /// NOMBRE DE LA CLASE: Ope_Notas_Credito_Detalle
        /// DESCRIPCIÓN: 
        /// PARÁMETROS :     
        /// CREO       : Miguel Angel AlvRAdo Enriquez
        /// FECHA_CREO : 24/Febrero/2014
        /// MODIFICO          :
        /// FECHA_MODIFICO    :
        /// CAUSA_MODIFICACIÓN:
        ///*******************************************************************************
        public class Ope_Notas_Credito_Detalle {
            public const String Tabla_Ope_Notas_Credito_Detalles = "OPE_NOTAS_CREDITO_DETALLES";
            public const String Campo_No_Nota_Credito = "NO_NOTA_CREDITO";
            public const String Campo_Producto_Id = "PRODUCTO_ID";
            public const String Campo_Serie = "SERIE";
            public const String Campo_No_Factura = "NO_FACTURA";
            public const String Campo_Serie_Factura = "SERIE_FACTURA";
            public const String Campo_Cantidad = "CANTIDAD";
            public const String Campo_Codigo = "CODIGO";
            public const String Campo_Cuenta_Predial = "CUENTA_PREDIAL";
            public const String Campo_Descripcion = "DESCRIPCION";
            public const String Campo_Unidad = "UNIDAD";
            public const String Campo_Porcentaje_Impuesto = "PORCENTAJE_IMPUESTO";
            public const String Campo_Subtotal_Detalle = "SUBTOTAL_DETALLE";
            public const String Campo_Precio_Unitario = "PRECIO_UNITARIO";
            public const String Campo_Iva = "IVA";
            public const String Campo_Total_Detalle = "TOTAL_DETALLE";
            public const String Campo_Usuario_Creo = "USUARIO_CREO";
            public const String Campo_Ip_Creo = "IP_CREO";
            public const String Campo_Equipo_Creo = "EQUIPO_CREO";
            public const String Campo_Fecha_Creo = "FECHA_CREO";
            public const String Campo_Usuario_Modifico = "USUARIO_MODIFICO";
            public const String Campo_Ip_Modifico = "IP_MODIFICO";
            public const String Campo_Equipo_Modifico = "EQUIPO_MODIFICO";
            public const String Campo_Fecha_Modifico = "FECHA_MODIFICO";
        }

        #endregion

}
