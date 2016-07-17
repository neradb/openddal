package com.openddal.server.frontend;


import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

import com.openddal.server.util.SecurityUtil;
import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * extract the common check privilege function.
 *
 * Created by snow_young on 16/7/17.
 */
public abstract class AbstractPrivilege implements Privilege{
    private static final Logger logger = LoggerFactory.getLogger(AbstractPrivilege.class);

    @Override
    public boolean hasPrivilege(String clientName, String clientPass, String salt) {
        try {
            String originPass = get(clientName);
            if(originPass == null || originPass.equals("")){
                return false;
            }
            String encryptPass411 = Base64.encodeBase64String(SecurityUtil.scramble411(originPass, salt));
            if(encryptPass411.equals(clientPass)){
                return true;
            }
        }catch (NoSuchAlgorithmException e) {
            logger.error("serious NoSuchAlgorithmException exception ", e);
        } catch (UnsupportedEncodingException e) {
            logger.error("serious UnsupportedEncodingException exception ", e);
        }
        return false;
    }

}
