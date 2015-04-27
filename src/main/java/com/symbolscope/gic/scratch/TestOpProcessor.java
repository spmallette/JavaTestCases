package com.symbolscope.gic.scratch;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.util.function.ThrowingConsumer;

import java.util.HashMap;
import java.util.Map;

/**
 * Trivial op processor.
 */
public class TestOpProcessor implements OpProcessor {

    @Override
    public String getName() {
        return "scratch";
    }


    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        String opname = message.getOp();
        Map<String, Object> params = message.getArgs();
        Map<String, Object> response = new HashMap<>();
        response.put("message", String.format("you requested the op %s with %d parameters", opname, params.size()));
        return context -> returnMap(context, response);
    }


    public static void returnMap(Context context, Map<String, Object> out) {
        if (!out.containsKey("status")) {
            out.put("status", "ok");
        }
        RequestMessage rqm = context.getRequestMessage();
        context.getChannelHandlerContext().writeAndFlush(ResponseMessage.build(rqm)
                .code(ResponseStatusCode.SUCCESS)
                .result(out)
                .create());
        context.getChannelHandlerContext().writeAndFlush(ResponseMessage.build(rqm).code(ResponseStatusCode.SUCCESS_TERMINATOR).create());
    }
}
