package etl.log;


import db.advanced.DBOperateAdvanced;
import db.advanced.DBOperateAdvancedImpl;
import exception.parameter.ParameterUseErrorException;
import exception.parameter.UnSupportedFunctionException;
import system.parameter.ProcedureParameter;

import java.sql.SQLException;

/**
 * @author TZ
 * @date 2019/8/19 11:38
 */
public class Log {
    private static DBOperateAdvanced dbOperate = new DBOperateAdvancedImpl();

    /**
     * 记录作业流日志
     *
     * @param I_FLOW_ID     作业流ID（必输）
     * @param I_FLOW_STATUS 作业流状态（0为失败、1为成功、2为开始、3为运行）必输
     * @param I_BATCH_TYPE  批次类型（1为正常运行，2为断点续跑）（必输）
     * @param I_BATCH_ID    批次号
     */
    public static void log_flow(String I_FLOW_ID,
                                String I_FLOW_STATUS,
                                String I_BATCH_TYPE,
                                String I_BATCH_ID) throws SQLException, UnSupportedFunctionException, ParameterUseErrorException {
        dbOperate.callProcedure("PKG_CTL_LOG", "P_CTL_LOG_FLOW",
                new ProcedureParameter(true, I_FLOW_ID),
                new ProcedureParameter(true, I_FLOW_STATUS),
                new ProcedureParameter(true, I_BATCH_TYPE),
                new ProcedureParameter(true, I_BATCH_ID));
    }

    /**
     * 记录作业流外部依赖日志
     *
     * @param I_FLOW_ID     作业流ID（必输）
     * @param I_FLOW_STATUS 作业流状态（2为开始、3为运行）必输
     * @param I_JOB_NAME    作业名
     * @param I_BATCH_ID    批次号
     */
    public static void log_outdep(String I_FLOW_ID,
                                  String I_FLOW_STATUS,
                                  String I_JOB_NAME,
                                  String I_BATCH_ID) {

    }

    /**
     * 运行情况写入作业步骤日志表或作业脚本
     *
     * @param I_JOB_NAME    作业名（必输）
     * @param I_BATCH_ID    批次号
     * @param I_JOB_STEP    作业步骤
     * @param I_RUN_STRING  运行字符串
     * @param I_RUN_CODE    运行代码
     * @param I_RUN_ERRM    运行错误
     * @param I_EXEC_NUM    执行记录数
     * @param I_SUCC_STATUS 成功标志
     * @param I_EXEC_MODE   执行模式（1为运行作业，2为生成脚本）
     * @param I_SCRIPT_FLAG 脚本标志（1为脚本、0为描述）
     */
    public static void log_step(String I_JOB_NAME, String I_BATCH_ID, String I_JOB_STEP,
                                String I_RUN_STRING, String I_RUN_CODE, String I_RUN_ERRM,
                                String I_EXEC_NUM, String I_SUCC_STATUS, int I_EXEC_MODE,
                                int I_SCRIPT_FLAG) throws SQLException, ParameterUseErrorException, UnSupportedFunctionException {

//        if (I_BATCH_ID == null) {
//            V_BATCH_ID = String.valueOf(getSequenceNextValue("SEQ_CTL_BATCH"));
//        } else {
//            V_BATCH_ID = I_BATCH_ID;
//        }
//        if (I_EXEC_MODE == 1) {
//            SQLType type = OracleType.VARCHAR2;
//            ProcedureParameter procedureParameter = new ProcedureParameter(false, type);
//            CallableStatement statement = dbOperate.callProcedure("OWA_UTIL", "WHO_CALLED_ME", procedureParameter, procedureParameter, procedureParameter, procedureParameter);
//            if (statement != null) {
//                V_CALLER_OWNER = statement.getString(1);
//                V_CALLER_NAME = statement.getString(2);
//                V_CALLER_LINE = statement.getString(3);
//                V_CALLER_CTGY = statement.getString(4);
//            } else {
//                V_CALLER_OWNER = null;
//                V_CALLER_NAME = null;
//                V_CALLER_LINE = null;
//                V_CALLER_CTGY = null;
//            }
//            FunctionParameter inParameter = new FunctionParameter(false, type);
//            FunctionParameter outParameter1 = new FunctionParameter(true, "USERENV");
//            FunctionParameter outParameter2 = new FunctionParameter(true, "SESSIONID");
//            statement = dbOperate.callFunction("", "SYS_CONTEXT", inParameter, outParameter1, outParameter2);
//            V_SESSION_ID = statement.getString(1);
//
//            V_STEP_ID = Tools.getSequenceNextValue("SEQ_CTL_STEP");
//
//            // 对V_CALLER_CTGY进行简化处理
//            if (V_CALLER_CTGY == null) {
//                V_CALLER_CTGY = "-?-";
//            } else {
//                switch (V_CALLER_CTGY) {
//                    case "PROCEDURE":
//                        V_CALLER_CTGY = "PRC";
//                        break;
//                    case "FUNCTION":
//                        V_CALLER_CTGY = "FNC";
//                        break;
//                    case "PACKAGE":
//                        V_CALLER_CTGY = "SPC";
//                        break;
//                    case "PACKAGE BODY":
//                        V_CALLER_CTGY = "BDY";
//                        break;
//                    case "ANONYMOUS BLOCK":
//                        V_CALLER_CTGY = "BLK";
//                        break;
//                    default:
//                        V_CALLER_CTGY = "-?-";
//                        break;
//                }
//            }
//            String[] targetColumns = {"STEP_ID",
//                    "BATCH_ID",
//                    "JOB_NAME",
//                    "JOB_STEP",
//                    "RUN_STRING",
//                    "RUN_CODE",
//                    "RUN_ERRM",
//                    "SUCC_STATUS",
//                    "EXEC_TIME",
//                    "EXEC_NUM",
//                    "SESSION_ID",
//                    "LOGGER_CTGY",
//                    "LOGGER_OWNER",
//                    "LOGGER_NAME",
//                    "LOGGER_LINE"};
//            Object[] sourceData = {
//                    V_STEP_ID,
//                    V_BATCH_ID,
//                    I_JOB_NAME,
//                    I_JOB_STEP,
//                    I_RUN_STRING,
//                    I_RUN_CODE,
//                    I_RUN_ERRM, nvl(I_SUCC_STATUS, 1),
//                    sysdate(),
//                    I_EXEC_NUM,
//                    V_SESSION_ID,
//                    V_CALLER_CTGY,
//                    V_CALLER_OWNER,
//                    V_CALLER_NAME,
//                    V_CALLER_LINE};
//            dbOperate.insertFromSourceDate("T_CTL_LOG_STEP", sourceData, targetColumns);
//            dbOperate.commit();
//        } else {
//            ResultSet resultSet = dbOperate.select("SELECT COUNT(*) + 1 FROM T_CTL_LOG_SCRIPT A WHERE A.JOB_NAME = '" + I_JOB_NAME + "' AND A.BATCH_ID = '" + I_BATCH_ID + "'");
//            resultSet.next();
//            V_STEP_NUM = resultSet.getInt(1);
//            Object[] sourceDate = {V_BATCH_ID, V_STEP_NUM, I_JOB_NAME, I_JOB_STEP, I_RUN_STRING, I_SCRIPT_FLAG, sysdate(), "ADMIN"};
//            String[] targetColumns = {
//                    "BATCH_ID",
//                    "STEP_NUM",
//                    "JOB_NAME",
//                    "JOB_STEP",
//                    "RUN_STRING",
//                    "SCRIPT_FLAG",
//                    "CREATE_TIME",
//                    "CREATE_USER"};
//            dbOperate.insertFromSourceDate("T_CTL_LOG_SCRIPT", sourceDate, targetColumns);
//            dbOperate.commit();
//        }
        dbOperate.callProcedure("PKG_CTL_LOG", "P_CTL_LOG_SETP",
                new ProcedureParameter(true, I_JOB_NAME),
                new ProcedureParameter(true, I_BATCH_ID),
                new ProcedureParameter(true, I_JOB_STEP),
                new ProcedureParameter(true, I_RUN_STRING),
                new ProcedureParameter(true, I_RUN_CODE),
                new ProcedureParameter(true, I_RUN_ERRM),
                new ProcedureParameter(true, I_EXEC_NUM),
                new ProcedureParameter(true, I_SUCC_STATUS),
                new ProcedureParameter(true, I_EXEC_MODE),
                new ProcedureParameter(true, I_SCRIPT_FLAG));
    }

    public static void log_step(String I_JOB_NAME, String I_BATCH_ID, String I_JOB_STEP,
                                String I_RUN_STRING, String I_RUN_CODE, String I_RUN_ERRM,
                                String I_EXEC_NUM, String I_SUCC_STATUS) throws UnSupportedFunctionException, SQLException, ParameterUseErrorException {
        log_step(I_JOB_NAME, I_BATCH_ID, I_JOB_STEP, I_RUN_STRING, I_RUN_CODE,
                I_RUN_ERRM, I_EXEC_NUM, I_SUCC_STATUS, 1, 0);
    }

    /**
     * 运行情况写入作业状态日志表
     *
     * @param I_JOB_NAME   作业名（必输）
     * @param I_JOB_STATUS 成功标志（0为失败、1为成功、2为运行、3为未启用）必输
     * @param I_BATCH_ID   批次号
     */
    public static void log_stat(String I_JOB_NAME, String I_JOB_STATUS, String I_BATCH_ID) throws SQLException, ParameterUseErrorException, UnSupportedFunctionException {
        dbOperate.callProcedure("PKG_CTL_LOG", "P_CTL_LOG_STAT",
                new ProcedureParameter(true, I_JOB_NAME),
                new ProcedureParameter(true, I_JOB_STATUS),
                new ProcedureParameter(true, I_BATCH_ID));
    }

    /**
     * 运行情况写入作业子进程日志表
     *
     * @param I_JOB_NAME      作业名（必输）
     * @param I_BATCH_ID      批次号（必输）
     * @param I_SUBPRO_NAME   作业子进程
     * @param I_JOB_SUBNAME   子作业
     * @param I_SUBPRO_STRING 作业子进程源串
     * @param I_SUBPRO_STATUS 子进程状态（0为失败、1为成功、2为运行、4未运行、5为超时）
     * @param I_SUBPRO_ERRM   运行错误
     * @param I_EXEC_NUM      执行记录数
     * @param I_SUBPRO_COUNT  作业子进程合计数
     */
    public static void log_subpro(String I_JOB_NAME,
                                  String I_BATCH_ID,
                                  String I_SUBPRO_NAME,
                                  String I_JOB_SUBNAME,
                                  String I_SUBPRO_STRING,
                                  String I_SUBPRO_STATUS,
                                  String I_SUBPRO_ERRM,
                                  int I_EXEC_NUM,
                                  int I_SUBPRO_COUNT) throws ParameterUseErrorException, SQLException, UnSupportedFunctionException {
        dbOperate.callProcedure("PKG_CTL_LOG", "P_CTL_LOG_SUBPRO",
                new ProcedureParameter(true, I_JOB_NAME),
                new ProcedureParameter(true, I_BATCH_ID),
                new ProcedureParameter(true, I_SUBPRO_NAME),
                new ProcedureParameter(true, I_JOB_SUBNAME),
                new ProcedureParameter(true, I_SUBPRO_STRING),
                new ProcedureParameter(true, I_SUBPRO_STATUS),
                new ProcedureParameter(true, I_SUBPRO_ERRM),
                new ProcedureParameter(true, I_EXEC_NUM),
                new ProcedureParameter(true, I_SUBPRO_COUNT));
    }
}
