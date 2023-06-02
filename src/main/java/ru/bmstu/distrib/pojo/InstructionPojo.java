package ru.bmstu.distrib.pojo;

import db.instructions.Instruction;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author vlad333rrty
 */
public record InstructionPojo(
        @RequestParam("instruction_type") InstructionPojoType type,
        @RequestParam("data") byte[] data)
{
}
