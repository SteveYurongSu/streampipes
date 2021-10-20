/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.streampipes.rest.impl;

import org.apache.streampipes.model.client.user.*;
import org.apache.streampipes.model.message.Notifications;
import org.apache.streampipes.rest.core.base.impl.AbstractAuthGuardedRestResource;
import org.apache.streampipes.rest.shared.annotation.JacksonSerialized;
import org.apache.streampipes.user.management.service.TokenService;
import org.apache.streampipes.user.management.util.PasswordUtil;
import org.springframework.security.core.context.SecurityContextHolder;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Path("/v2/users")
public class UserResource extends AbstractAuthGuardedRestResource {

  @GET
  @JacksonSerialized
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllUsers(@QueryParam("type") String principalType) {
    List<Principal> allPrincipals = new ArrayList<>();
    if (principalType != null && principalType.equals(PrincipalType.USER_ACCOUNT.name())) {
      allPrincipals.addAll(getUserStorage().getAllUserAccounts());
    } else if (principalType != null && principalType.equals(PrincipalType.SERVICE_ACCOUNT.name())) {
      allPrincipals.addAll(getUserStorage().getAllServiceAccounts());
    } else {
      allPrincipals.addAll(getUserStorage().getAllUsers());
    }
    removeCredentials(allPrincipals);
    return ok(allPrincipals);
  }


  @GET
  @JacksonSerialized
  @Path("{principalId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getUserDetails(@PathParam("principalId") String principalId) {
    Principal principal = getPrincipalById(principalId);
    removeCredentials(principal);

    if (principal != null) {
      return ok(principal);
    } else {
      return statusMessage(Notifications.error("User not found"));
    }
  }

  @DELETE
  @JacksonSerialized
  @Path("{principalId}")
  public Response deleteUser(@PathParam("principalId") String principalId) {
    Principal principal = getPrincipalById(principalId);

    if (principal != null) {
      getUserStorage().deleteUser(principalId);
      return ok();
    } else {
      return statusMessage(Notifications.error("User not found"));
    }
  }

  @Path("{userId}/appearance/mode/{darkMode}")
  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateAppearanceMode(@PathParam("userId") String userId,
                                       @PathParam("darkMode") boolean darkMode) {
    String authenticatedUserId = getAuthenticatedUsername();
    if (authenticatedUserId != null) {
      UserAccount user = getUser(authenticatedUserId);
      user.setDarkMode(darkMode);
      getUserStorage().updateUser(user);

      return ok(Notifications.success("Appearance updated"));
    } else {
      return statusMessage(Notifications.error("User not found"));
    }
  }

  @POST
  @Path("/user")
  @JacksonSerialized
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response registerUser(UserAccount userAccount) {
    // TODO check if userId is already taken
    try {
      if (getUserStorage().getUser(userAccount.getUsername()) == null) {
        String property = userAccount.getPassword();
        String encryptedProperty = PasswordUtil.encryptPassword(property);
        userAccount.setPassword(encryptedProperty);
        getUserStorage().storeUser(userAccount);
        return ok();
      } else {
        return badRequest(Notifications.error("This user ID already exists. Please choose another address."));
      }
    } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
      return badRequest();
    }
  }

  @POST
  @Path("/service")
  @JacksonSerialized
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response registerService(ServiceAccount userAccount) {
    // TODO check if userId is already taken
    if (getUserStorage().getUser(userAccount.getUsername()) == null) {
      getUserStorage().storeUser(userAccount);
      return ok();
    } else {
      return badRequest(Notifications.error("This user ID already exists. Please choose another address."));
    }
  }

  @POST
  @Path("{userId}/tokens")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @JacksonSerialized
  public Response createNewApiToken(@PathParam("userId") String userId,
                                    RawUserApiToken rawToken) {
    RawUserApiToken generatedToken = new TokenService().createAndStoreNewToken(userId, rawToken);
    return ok(generatedToken);
  }

  @PUT
  @Path("user/{principalId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateUserAccountDetails(@PathParam("principalId") String principalId,
                                    UserAccount user) {
    String authenticatedUserId = getAuthenticatedUsername();
    if (user != null && (authenticatedUserId.equals(principalId) || isAdmin())) {
      Principal existingUser = getPrincipalById(principalId);
      updateUser((UserAccount) existingUser, user);
      user.setRev(existingUser.getRev());
      getUserStorage().updateUser(user);
      return ok(Notifications.success("User updated"));
    } else {
      return statusMessage(Notifications.error("User not found"));
    }
  }

  @PUT
  @Path("service/{principalId}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response updateServiceAccountDetails(@PathParam("principalId") String principalId,
                                           ServiceAccount user) {
    String authenticatedUserId = getAuthenticatedUsername();
    if (user != null && (authenticatedUserId.equals(principalId) || isAdmin())) {
      Principal existingUser = getPrincipalById(principalId);
      user.setRev(existingUser.getRev());
      getUserStorage().updateUser(user);
      return ok(Notifications.success("User updated"));
    } else {
      return statusMessage(Notifications.error("User not found"));
    }
  }

  private boolean isAdmin() {
    return SecurityContextHolder.getContext().getAuthentication().getAuthorities().stream().anyMatch(r -> r.getAuthority().equals(Role.ROLE_ADMIN.name()));
  }

  private void updateUser(UserAccount existingUser, UserAccount user) {
    user.setPassword(existingUser.getPassword());
    user.setUserApiTokens(existingUser
            .getUserApiTokens()
            .stream()
            .filter(existingToken -> user.getUserApiTokens()
                    .stream()
                    .anyMatch(updatedToken -> existingToken
                            .getTokenId()
                            .equals(updatedToken.getTokenId())))
            .collect(Collectors.toList()));
  }

  private UserAccount getUser(String username) {
    return getUserStorage().getUserAccount(username);
  }

  private Principal getPrincipal(String username) {
    return getUserStorage().getUser(username);
  }

  private Principal getPrincipalById(String principalId) {
    return getUserStorage().getUserById(principalId);
  }

  private void removeCredentials(List<Principal> principals) {
    principals.forEach(this::removeCredentials);
  }

  private void removeCredentials(Principal principal) {
    if (principal instanceof UserAccount) {
      ((UserAccount) principal).setPassword("");
    }
  }
}
