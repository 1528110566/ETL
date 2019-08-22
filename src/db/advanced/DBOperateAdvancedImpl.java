package db.advanced;

import db.DBOperateImpl;
import db.DBUtil;
import exception.db.ConnectException;
import exception.db.FuncCallException;
import exception.db.StupidCallingException;
import exception.parameter.UnSupportedFunctionException;
import system.parameter.FunctionParameter;
import system.parameter.ProcedureParameter;

import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * @author TZ
 * @date 2019/8/20 19:25
 */
public class DBOperateAdvancedImpl extends DBOperateImpl implements DBOperateAdvanced {
    private static DBUtil dbUtil;

    static {
        try {
            dbUtil = DBUtil.getInstance();
        } catch (ConnectException | SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public CallableStatement callProcedure(String procPackageName, String procName, ProcedureParameter... parameters) throws SQLException, UnSupportedFunctionException {
        // 拼接调用字符串
        StringBuilder builder = new StringBuilder();
        builder.append("{ call ");
        if (!"".equals(procPackageName) && procPackageName != null) {
            builder.append(procPackageName).append(".");
        }
        builder.append(procName).append("(");
        for (int i = 0; i < parameters.length; i++) {
            if (i == 0) {
                builder.append("?");
            } else {
                builder.append(",?");
            }
        }
        builder.append(") }");
        // 调试用
        System.out.println(builder.toString());
        // 调用存储过程
        CallableStatement statement = dbUtil.getConnection().prepareCall(builder.toString());
        // 设置存储过程参数
        for (int i = 0; i < parameters.length; i++) {
            // 如果是in类型的参数
            if (parameters[i].isIn()) {
                statement.setString(i + 1, parameters[i].getStr());
            } else {
                statement.registerOutParameter(i + 1, parameters[i].getTypes());
            }
        }
        statement.execute();
        return statement;
    }

    @Override
    public CallableStatement callFunction(String funcPackageName, String funcName, FunctionParameter... parameters) throws SQLException, UnSupportedFunctionException, FuncCallException {
        if (parameters[0].isIn()) {
            throw new FuncCallException("调用函数时，第一个参数必须是out类型，用作返回值");
        }
        // 拼接调用字符串
        StringBuilder builder = new StringBuilder();
        builder.append("{? = call ");
        if (!"".equals(funcPackageName) && funcPackageName != null) {
            builder.append(funcPackageName).append(".");
        }
        builder.append(funcName).append("(");
        for (int i = 1; i < parameters.length; i++) {
            if (i == 1) {
                builder.append("?");
            } else {
                builder.append(",?");
            }
        }
        builder.append(") }");
        // 调试用
        System.out.println(builder.toString());
        // 调用函数
        CallableStatement statement = dbUtil.getConnection().prepareCall(builder.toString());
        // 设置函数参数
        statement.registerOutParameter(1, parameters[0].getTypes());
        for (int i = 1; i < parameters.length; i++) {
            // 如果是in类型的参数
            if (parameters[i].isIn()) {
                statement.setString(i + 1, parameters[i].getStr());
            } else {
                statement.registerOutParameter(i + 1, parameters[i].getTypes());
            }
        }
        statement.execute();
        return statement;
    }

    @Override
    public int insertFromSourceTable(String sourceTableName, String targetTableName, String[] sourceColumns, String[] targetColumns, String whereCondition) throws StupidCallingException {
        // 异常处理
        if (sourceTableName == null || "".equals(sourceTableName)) {
            throw new StupidCallingException("没有输入源表");
        }
        if (targetTableName == null || "".equals(targetTableName)) {
            throw new StupidCallingException("没有输入目标表");
        }
        if (sourceColumns == null || sourceColumns.length == 0) {
            throw new StupidCallingException("没有输入源表列名");
        }
        if (targetColumns == null || targetColumns.length == 0) {
            throw new StupidCallingException("没有输入目标表列名");
        }
        if (sourceColumns.length != targetColumns.length) {
            throw new StupidCallingException("列的个数不匹配");
        }
        // 拼接SQL
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ").append(targetTableName).append("(").append(targetColumns[0]);
        // 目标表字段拼接
        for (int i = 1; i < targetColumns.length; i++) {
            builder.append(",").append(targetColumns[i]);
        }
        builder.append(")").append(" select ").append(sourceColumns[0]);
        // 源表字段拼接
        for (int i = 1; i < sourceColumns.length; i++) {
            builder.append(",").append(sourceColumns[i]);
        }
        builder.append(" from ").append(sourceTableName);
        // where条件拼接
        if (!"".equals(whereCondition) && whereCondition != null) {
            if (whereCondition.trim().startsWith("where")) {
                whereCondition = whereCondition.trim().replace("where", "");
            }
            builder.append(" where ").append(whereCondition);
        }
        // 调试用
        System.out.println(builder.toString());
        return dbUtil.sendSQL(builder.toString());
    }

    @Override
    public int insertFromSourceTable(String sourceTableName, String targetTableName, String[] sourceColumns, String[] targetColumns) throws StupidCallingException {
        return insertFromSourceTable(sourceTableName, targetTableName, sourceColumns, targetColumns, "");
    }

    @Override
    public int insertFromSourceDate(String targetTableName, Object[] sourceData, String[] targetColumns) throws StupidCallingException, SQLException {
        // 异常处理
        if (targetTableName == null | "".equals(targetTableName)) {
            throw new StupidCallingException("没有输入目标列");
        }
        if (sourceData == null || sourceData.length == 0) {
            throw new StupidCallingException("没有输入源数据");
        }
        if (targetColumns == null || targetColumns.length == 0) {
            throw new StupidCallingException("没有输入目标列");
        }
        if (sourceData.length != targetColumns.length) {
            throw new StupidCallingException("数据长度不匹配");
        }
        CallableStatement statement = join(targetTableName, sourceData, targetColumns);
        return statement.executeUpdate();
    }

    private CallableStatement join(String targetTableName, Object[] sourceData, String[] targetColumns) throws SQLException {
        // TODO
        System.out.println("拼接SQL");
        // 拼接SQL
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ").append(targetTableName).append(" (").append(targetColumns[0]);
        for (int i = 1; i < targetColumns.length; i++) {
            builder.append(",").append(targetColumns[i]);
        }
        builder.append(") values (?");
        for (int i = 1; i < sourceData.length; i++) {
            builder.append(",?");
        }
        builder.append(")");
        // TODO
        System.out.println("SQL拼接完成:" + builder.toString());

        CallableStatement statement = dbUtil.getConnection().prepareCall(builder.toString());
        for (int i = 0; i < sourceData.length; i++) {
            // TODO
            System.out.println((i + 1) + "\t" + sourceData[i]);
            statement.setObject(i + 1, sourceData[i]);
        }
        return statement;
    }
}
