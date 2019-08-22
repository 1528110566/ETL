package log;


import db.advanced.DBOperateAdvanced;
import db.advanced.DBOperateAdvancedImpl;
import exception.db.FuncCallException;
import exception.db.StupidCallingException;
import exception.function.FunctionArgumentTypeException;
import exception.parameter.ParameterUseErrorException;
import exception.parameter.UnSupportedFunctionException;
import oracle.jdbc.OracleType;
import system.parameter.FunctionParameter;
import system.parameter.ProcedureParameter;
import tool.Tools;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;

import static system.function.Function.nvl;
import static system.function.Function.sysdate;
import static tool.Tools.getSequenceNextValue;

/**
 * @author TZ
 * @date 2019/8/19 11:38
 */
public class Log {
    private static DBOperateAdvanced dbOperate = new DBOperateAdvancedImpl();

    /**
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
                                int I_SCRIPT_FLAG) throws SQLException, ParameterUseErrorException, UnSupportedFunctionException, FuncCallException, StupidCallingException, FunctionArgumentTypeException {
        int V_STEP_ID;
        String V_CALLER_CTGY;
        String V_CALLER_OWNER;
        String V_CALLER_NAME;
        String V_CALLER_LINE;
        String V_BATCH_ID;
        String V_SESSION_ID;
        int V_STEP_NUM;

        if (I_BATCH_ID == null) {
            V_BATCH_ID = String.valueOf(getSequenceNextValue("SEQ_CTL_BATCH"));
        } else {
            V_BATCH_ID = I_BATCH_ID;
        }
        if (I_EXEC_MODE == 1) {
            SQLType type = OracleType.VARCHAR2;
            ProcedureParameter procedureParameter = new ProcedureParameter(false, type);
            CallableStatement statement = dbOperate.callProcedure("OWA_UTIL", "WHO_CALLED_ME", procedureParameter, procedureParameter, procedureParameter, procedureParameter);
            if (statement != null) {
                V_CALLER_OWNER = statement.getString(1);
                V_CALLER_NAME = statement.getString(2);
                V_CALLER_LINE = statement.getString(3);
                V_CALLER_CTGY = statement.getString(4);
            } else {
                V_CALLER_OWNER = null;
                V_CALLER_NAME = null;
                V_CALLER_LINE = null;
                V_CALLER_CTGY = null;
            }
            FunctionParameter inParameter = new FunctionParameter(false, type);
            FunctionParameter outParameter1 = new FunctionParameter(true, "USERENV");
            FunctionParameter outParameter2 = new FunctionParameter(true, "SESSIONID");
            statement = dbOperate.callFunction("", "SYS_CONTEXT", inParameter, outParameter1, outParameter2);
            V_SESSION_ID = statement.getString(1);

            V_STEP_ID = Tools.getSequenceNextValue("SEQ_CTL_STEP");

            // 对V_CALLER_CTGY进行简化处理
            if (V_CALLER_CTGY == null) {
                V_CALLER_CTGY = "-?-";
            } else {
                switch (V_CALLER_CTGY) {
                    case "PROCEDURE":
                        V_CALLER_CTGY = "PRC";
                        break;
                    case "FUNCTION":
                        V_CALLER_CTGY = "FNC";
                        break;
                    case "PACKAGE":
                        V_CALLER_CTGY = "SPC";
                        break;
                    case "PACKAGE BODY":
                        V_CALLER_CTGY = "BDY";
                        break;
                    case "ANONYMOUS BLOCK":
                        V_CALLER_CTGY = "BLK";
                        break;
                    default:
                        V_CALLER_CTGY = "-?-";
                        break;
                }
            }
            String[] targetColumns = {"STEP_ID",
                    "BATCH_ID",
                    "JOB_NAME",
                    "JOB_STEP",
                    "RUN_STRING",
                    "RUN_CODE",
                    "RUN_ERRM",
                    "SUCC_STATUS",
                    "EXEC_TIME",
                    "EXEC_NUM",
                    "SESSION_ID",
                    "LOGGER_CTGY",
                    "LOGGER_OWNER",
                    "LOGGER_NAME",
                    "LOGGER_LINE"};
            Object[] sourceData = {
                    V_STEP_ID,
                    V_BATCH_ID,
                    I_JOB_NAME,
                    I_JOB_STEP,
                    I_RUN_STRING,
                    I_RUN_CODE,
                    I_RUN_ERRM, nvl(I_SUCC_STATUS, 1),
                    sysdate(),
                    I_EXEC_NUM,
                    V_SESSION_ID,
                    V_CALLER_CTGY,
                    V_CALLER_OWNER,
                    V_CALLER_NAME,
                    V_CALLER_LINE};
            dbOperate.insertFromSourceDate("SJJC_BZ.T_CTL_LOG_STEP", sourceData, targetColumns);
            dbOperate.commit();
        } else {
            ResultSet resultSet = dbOperate.select("SELECT COUNT(*) + 1 FROM SJJC_BZ.T_CTL_LOG_SCRIPT A WHERE A.JOB_NAME = '" + I_JOB_NAME + "' AND A.BATCH_ID = '" + I_BATCH_ID + "'");
            resultSet.next();
            V_STEP_NUM = resultSet.getInt(1);
            Object[] sourceDate = {V_BATCH_ID, V_STEP_NUM, I_JOB_NAME, I_JOB_STEP, I_RUN_STRING, I_SCRIPT_FLAG, sysdate(), "ADMIN"};
            String[] targetColumns = {
                    "BATCH_ID",
                    "STEP_NUM",
                    "JOB_NAME",
                    "JOB_STEP",
                    "RUN_STRING",
                    "SCRIPT_FLAG",
                    "CREATE_TIME",
                    "CREATE_USER"};
            dbOperate.insertFromSourceDate("SJJC_BZ.T_CTL_LOG_SCRIPT", sourceDate, targetColumns);
            dbOperate.commit();
        }
    }
}
