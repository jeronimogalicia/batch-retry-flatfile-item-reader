package com.demo.spring.batch;

import com.demo.spring.batch.model.Player;
import org.springframework.batch.item.ItemProcessor;

public class MyProcessor implements ItemProcessor<Player, Player> {

	@Override
	public Player process(final Player arg0) throws Exception {
		System.out.println("MyProcessor : " + arg0.getFirstName());
		return arg0;
	}

}
