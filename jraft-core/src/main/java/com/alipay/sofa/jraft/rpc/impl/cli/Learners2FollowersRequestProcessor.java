/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alipay.sofa.jraft.rpc.impl.cli;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.error.RaftError;
import com.alipay.sofa.jraft.rpc.CliRequests.RemoveLearnersRequest;
import com.alipay.sofa.jraft.rpc.RpcRequestClosure;
import com.alipay.sofa.jraft.util.RpcFactoryHelper;
import com.google.protobuf.Message;

import static com.alipay.sofa.jraft.rpc.CliRequests.Learners2FollowersRequest;
import static com.alipay.sofa.jraft.rpc.CliRequests.Learners2FollowersResponse;

/**
 * Learners2Followers request processor.
 *
 * @author jianbin.chen (364176773@qq.com)
 *
 */
public class Learners2FollowersRequestProcessor extends BaseCliRequestProcessor<Learners2FollowersRequest> {

    public Learners2FollowersRequestProcessor(final Executor executor) {
        super(executor, Learners2FollowersResponse.getDefaultInstance());
    }

    @Override
    protected String getPeerId(final Learners2FollowersRequest request) {
        return request.getLeaderId();
    }

    @Override
    protected String getGroupId(final Learners2FollowersRequest request) {
        return request.getGroupId();
    }

    @Override
    protected Message processRequest0(final CliRequestContext ctx, final Learners2FollowersRequest request,
                                      final RpcRequestClosure done) {
        final List<PeerId> oldConf = ctx.node.listPeers();
        final List<PeerId> oldLearners = ctx.node.listLearners();
        final List<PeerId> convertLearners = new ArrayList<>(request.getLearnersCount());

        for (final String peerStr : request.getLearnersList()) {
            final PeerId peer = new PeerId();
            if (!peer.parse(peerStr)) {
                return RpcFactoryHelper //
                    .responseFactory() //
                    .newResponse(defaultResp(), RaftError.EINVAL, "Fail to parse peer id %s", peerStr);
            }
            convertLearners.add(peer);
        }

        LOG.info("Receive Learners2FollowersRequest to {} from {}, convertLearners {}.", ctx.node.getNodeId(),
            done.getRpcCtx().getRemoteAddress(), convertLearners);
        ctx.node.learners2Followers(convertLearners, status -> {
            if (!status.isOk()) {
                done.run(status);
            } else {
                Learners2FollowersResponse.Builder rb = Learners2FollowersResponse.newBuilder();
                for (final PeerId peer : oldConf) {
                    rb.addOldPeers(peer.toString());
                }
                List<PeerId> conf = ctx.node.listPeers();
                for (final PeerId peer : conf) {
                    rb.addNewPeers(peer.toString());
                }
                for (final PeerId peer : oldLearners) {
                    rb.addOldLearners(peer.toString());
                }
                List<PeerId> newLearners = ctx.node.listLearners();
                for (final PeerId peer : newLearners) {
                    rb.addCurrentLearners(peer.toString());
                }
                done.sendResponse(rb.build());
            }
        });

        return null;
    }

    @Override
    public String interest() {
        return RemoveLearnersRequest.class.getName();
    }

}
